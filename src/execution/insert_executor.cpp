//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  auto catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  heap_ = table_info_->table_.get();
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  for (uint32_t i = 0; i < indexs_.size(); i++) {
    if (indexs_[i]->is_primary_key_) {
      primary_index_pos_ = i;
    }
  }
}

void InsertExecutor::Init() { child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (called_) {
    return false;
  }
  called_ = true;
  Tuple inserted_tuple;
  RID inserted_rid;
  int count = 0;
  auto txn = GetExecutorContext()->GetTransaction();
  auto txn_mgr = GetExecutorContext()->GetTransactionManager();

  // 有主键索引
  //  - 先看索引是否存在插入的key，存在则看是否被删除了。不是则抛出异常，是的话，则需要原地更新这个tuple。
  //      - 不存在的话先插入到table_heap，获得record_id,
  //      设置record的version_link的is_progress为true，然后插入数据，最后设置is_progress为false
  //  - 若无主键，那么直接插入到table_heap中，然后也不需要维护UndoLog
  // 更新别的索引
  while (child_executor_->Next(&inserted_tuple, &inserted_rid)) {
    RID r_rid;
    if (primary_index_pos_ != -1) {
      IndexInfo *index = indexs_[primary_index_pos_];
      auto schema = index->index_->GetKeySchema();
      std::vector<uint32_t> col_idxs;
      for (const auto &col : schema->GetColumns()) {
        col_idxs.emplace_back(table_info_->schema_.GetColIdx(col.GetName()));
      }

      Tuple key = inserted_tuple.KeyFromTuple(table_info_->schema_, *schema, col_idxs);
      std::vector<RID> result;
      index->index_->ScanKey(key, &result, txn);
      if (!result.empty()) {
        auto meta = heap_->GetTupleMeta(result[0]);
        if (!meta.is_deleted_) {
          txn->SetTainted();
          throw ExecutionException("tuple shoule be deleted at insert_executor");
        }
        r_rid = result[0];
        if (!MarkUndoVersionLink(exec_ctx_, r_rid)) {
          txn->SetTainted();
          throw ExecutionException("Write-write confilct at insert_executor");
        }

        meta.is_deleted_ = false;
        if (meta.ts_ != txn->GetTransactionId()) {
          UndoLog undo_log;
          undo_log.is_deleted_ = true;
          undo_log.ts_ = meta.ts_;
          auto ver_link = txn_mgr->GetVersionLink(r_rid);
          BUSTUB_ASSERT(ver_link->in_progress_, "ver_link->in_progress_ must be true");
          if (ver_link.has_value()) {
            undo_log.prev_version_ = ver_link.value().prev_;
          }
          auto undo_link = txn->AppendUndoLog(undo_log);
          ver_link->prev_ = undo_link;
          assert(txn_mgr->UpdateVersionLink(r_rid, ver_link));
        }

        meta.ts_ = txn->GetTransactionId();
        heap_->UpdateTupleInPlace(meta, inserted_tuple, r_rid);
        txn->AppendWriteSet(table_info_->oid_, r_rid);
        UnmarkUndoVersionLink(exec_ctx_, r_rid);
      } else {
        TupleMeta meta{txn->GetTransactionId(), false};
        auto result_rid = heap_->InsertTuple(meta, inserted_tuple);
        if (!result_rid.has_value()) {
          throw ExecutionException("InsertTuple error");
        }
        r_rid = result_rid.value();
        if (!MarkUndoVersionLink(exec_ctx_, r_rid)) {
          txn->SetTainted();
          throw ExecutionException("Write-write confilct at insert_executor");
        }

        if (!index->index_->InsertEntry(key, r_rid, txn)) {
          std::vector<RID> result;
          index->index_->ScanKey(key, &result, txn);

          txn->SetTainted();
          throw ExecutionException("failed in insert_index");
        }
        txn->AppendWriteSet(table_info_->oid_, r_rid);
        UnmarkUndoVersionLink(exec_ctx_, r_rid);
      }
    } else {
      TupleMeta meta{txn->GetTransactionId(), false};
      auto result_rid = heap_->InsertTuple(meta, inserted_tuple);
      if (!result_rid.has_value()) {
        throw ExecutionException("InsertTuple error");
      }
      r_rid = result_rid.value();
      txn->AppendWriteSet(table_info_->oid_, r_rid);
    }

    for (auto index : indexs_) {
      auto schema = index->index_->GetKeySchema();
      std::vector<uint32_t> col_idxs;
      for (const auto &col : schema->GetColumns()) {
        col_idxs.emplace_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      Tuple key = inserted_tuple.KeyFromTuple(table_info_->schema_, *schema, col_idxs);
      if (!index->is_primary_key_) {
        std::vector<RID> result;
        /// For this semester 2023fall, the hash table is intend to support only unique keys. This means that the hash table should return false if the user tries to insert duplicate keys.
        assert(result.size() <= 1);
        index->index_->ScanKey(key, &result, txn);
        if (!result.empty()) {
          index->index_->DeleteEntry(key, r_rid, txn);
        }
        if (!index->index_->InsertEntry(key, r_rid, txn)) {
          txn->SetTainted();
          throw ExecutionException("Write-write confilct in insert_index");
        };
      }
    }
    count++;
  }

  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(count));

  *tuple = Tuple(values, &GetOutputSchema());
  *rid = RID{};
  return true;
}

}  // namespace bustub
