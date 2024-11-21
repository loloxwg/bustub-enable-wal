//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cassert>
#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "fmt/core.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

class CheckUpdateObject {
public:
  explicit CheckUpdateObject(const TupleMeta &meta, const Tuple &tuple, RID rid) : tuple_(tuple), rid_(rid){

  }

  auto operator() (const TupleMeta &meta, const Tuple &tuple, RID rid) -> bool {
    return IsTupleContentEqual(tuple_, tuple);
  }
private:
  //const TupleMeta &meta_;
  const Tuple &tuple_;
  RID rid_;

};


  

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);

  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  auto catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  heap_ = table_info_->table_.get();
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  for (const auto &index : indexs_) {
    if (index->is_primary_key_) {
      primary_key_index_ = index;
      break;
    }
  }

  modify_primary_key_ = false;
  if (primary_key_index_ != nullptr) {
    auto schema = table_info_->schema_;
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      auto& col = schema.GetColumn(i);
      auto expr = plan_->target_expressions_[i];
      if (primary_key_index_->key_schema_.TryGetColIdx(col.GetName()).has_value()) {
        if (dynamic_cast<ColumnValueExpression*>(expr.get()) == nullptr) {
          modify_primary_key_ = true;
          break;
        }
        auto cv_expr = dynamic_cast<ColumnValueExpression*>(expr.get());
        if (cv_expr->GetColIdx() != i) {
          modify_primary_key_ = true;
          break;
        } 
      }
    }
  }
}

// create table t1(a int primary key)
// insert into t1 values(1), (2), (3), (4)
// begin; 开启事务update
// update t1 set a = a + 1;
// 那么这个事务通过index scan能获取事务最新的数据
// 别的事务能通过index scan获取这个事务修改前的数据

// update 
// update本事务修改过的元组,修改本事务之前添加的UndoLog---可能,也可能没有,比如insert - update那就没有UndoLog.要是update - update那就是有
// update本事务没修改的元组,需要添加UndoLog
// Update有没有修改了主键
//  -1 若修改了主键，那么为了别的事务能够index scan修改前的数据，需要把新的数据插入到table_meta，并且修改原来元组的元信息的is_deleted为true，但是不删除索引，而是添加索引
//  -2 若没有修改主键，那么
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (called_) {
    return false;
  }
  called_ = true;
  int count = 0;
  Tuple updated_tuple;
  RID updated_rid;
  auto txn_mgr = GetExecutorContext()->GetTransactionManager();
  auto txn = GetExecutorContext()->GetTransaction();

  if (!modify_primary_key_) {
    while (child_executor_->Next(&updated_tuple, &updated_rid)) {
      auto meta = heap_->GetTupleMeta(updated_rid);
      if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
        txn->SetTainted();
        throw ExecutionException(fmt::format("update_executor failed for {} in txn{}, read_ts{}", meta.ts_,
                                            txn->GetTransactionIdHumanReadable(), txn->GetReadTs()));
      }
      if (!MarkUndoVersionLink(exec_ctx_, updated_rid)) {
        txn->SetTainted();
        throw ExecutionException(fmt::format("update_executor failed for {} in txn{}, read_ts{}", meta.ts_,
                                            txn->GetTransactionIdHumanReadable(), txn->GetReadTs()));
      }
      txn->AppendWriteSet(table_info_->oid_, updated_rid);

      const Schema *schema = &child_executor_->GetOutputSchema();
      std::vector<Value> values;
      for (const auto &expr : plan_->target_expressions_) {
        values.emplace_back(expr->Evaluate(&updated_tuple, child_executor_->GetOutputSchema()));
      }
      Tuple new_tuple(values, schema);

      auto prev_link = txn_mgr->GetUndoLink(updated_rid);
      if (meta.ts_ != txn->GetTransactionTempTs()) {
        auto undo_log = GenerateUndoLog(new_tuple, updated_tuple, schema, meta.ts_, prev_link);
        auto undo_link = txn->AppendUndoLog(undo_log);
        auto ver_link = VersionUndoLink::FromOptionalUndoLink(undo_link);
        ver_link->in_progress_ = true;
        assert(txn_mgr->UpdateVersionLink(updated_rid, ver_link));
      } else if (prev_link.has_value() && prev_link->IsValid() && prev_link->prev_log_idx_ != -1) {
        auto old_undo_log = txn_mgr->GetUndoLog(prev_link.value());
        auto old_tuple = ReconstructTuple(schema, updated_tuple, meta, {old_undo_log});
        if (old_tuple.has_value()) {
          auto undo_log = GenerateUndoLog(new_tuple, old_tuple.value(), old_undo_log, schema, old_undo_log.ts_,
                                          old_undo_log.prev_version_);
          auto prev_txn = txn_mgr->txn_map_[prev_link->prev_txn_];
          prev_txn->ModifyUndoLog(prev_link->prev_log_idx_, undo_log);
        }
      }

      meta.ts_ = txn->GetTransactionTempTs();
      CheckUpdateObject ch(meta, updated_tuple, updated_rid);

      if (!heap_->UpdateTupleInPlace(meta, new_tuple, updated_rid, ch)) {
        txn->SetTainted();
        UnmarkUndoVersionLink(exec_ctx_, updated_rid);
        throw ExecutionException(fmt::format("update_executor failed UpdateTupleInPlace for {} in txn{}, read_ts{}", meta.ts_,
                                            txn->GetTransactionIdHumanReadable(), txn->GetReadTs()));            
      }
      UnmarkUndoVersionLink(exec_ctx_, updated_rid);
      count++;
    }
  } else {
    
    std::vector<Tuple> tuples;
    std::vector<RID> rids;
    auto schema = child_executor_->GetOutputSchema();
    // 首先删除原来的元组，并且计算需要更新之后插入的元组
    while (child_executor_->Next(&updated_tuple, &updated_rid)) {
      auto meta = heap_->GetTupleMeta(updated_rid);
      if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
        txn->SetTainted();
        throw ExecutionException(fmt::format("update_executor failed for {} in txn{}, read_ts{}", meta.ts_,
                                            txn->GetTransactionIdHumanReadable(), txn->GetReadTs()));
      }
      if (!MarkUndoVersionLink(exec_ctx_, updated_rid)) {
        txn->SetTainted();
        throw ExecutionException(fmt::format("update_executor failed for {} in txn{}, read_ts{}", meta.ts_,
                                            txn->GetTransactionIdHumanReadable(), txn->GetReadTs()));
      }

      std::vector<Value> values;
      for (const auto &expr : plan_->target_expressions_) {
        values.emplace_back(expr->Evaluate(&updated_tuple, child_executor_->GetOutputSchema()));
      }
      Tuple new_tuple(values, &schema);
      tuples.emplace_back(new_tuple);
      rids.emplace_back(updated_rid);

      // 本事务插入——修改——删除，那就不应该有undo_log
      // 本事务修改的——删除，那就只能有一个undo_log
      if (meta.ts_ == txn->GetTransactionTempTs()) {
        auto undo_link = txn_mgr->GetUndoLink(updated_rid);
        if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_log_idx_ != -1 && undo_link->prev_txn_ == txn->GetTransactionId()) {
          UndoLog undo_log = txn->GetUndoLog(undo_link->prev_log_idx_);
          auto origin_tuple = ReconstructTuple(&schema, updated_tuple, meta, {undo_log});
          if (origin_tuple.has_value()) {
            std::vector<bool> modified_fields;
            for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
              modified_fields.emplace_back(true);
            }
            undo_log.modified_fields_ = modified_fields;
            undo_log.tuple_ = origin_tuple.value();
            txn->ModifyUndoLog(undo_link->prev_log_idx_, undo_log);
          }
        }
      } else {
        UndoLog undo_log;
        undo_log.is_deleted_ = false;
        std::vector<bool> modified_fields;
        for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
          modified_fields.emplace_back(true);
        }
        undo_log.modified_fields_ = modified_fields;
        undo_log.tuple_ = updated_tuple;
        undo_log.ts_ = meta.ts_;

        auto undo_link = txn_mgr->GetUndoLink(updated_rid);
        if (undo_link.has_value()) {
          undo_log.prev_version_ = undo_link.value();
        }
        txn_mgr->UpdateUndoLink(updated_rid, txn->AppendUndoLog(undo_log));
      }

      txn->AppendWriteSet(table_info_->oid_, updated_rid);
      meta.is_deleted_ = true;
      meta.ts_ = txn->GetTransactionTempTs();
      heap_->UpdateTupleMeta(meta, updated_rid);
      UnmarkUndoVersionLink(exec_ctx_, updated_rid);
      count++;
    }

    // 开始插入新的元组并且需要维护相关的UndoLog
    for (auto &tuple : tuples) {
      auto primary_schema = primary_key_index_->index_->GetKeySchema();
      std::vector<uint32_t> col_idxs;
      for (const auto &col : primary_schema->GetColumns()) {
        col_idxs.emplace_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      Tuple key = tuple.KeyFromTuple(table_info_->schema_, *primary_schema, col_idxs);
      std::vector<RID> result;
      primary_key_index_->index_->ScanKey(key, &result, txn);

      if (result.empty()) {
        auto new_rid = heap_->InsertTuple({txn->GetTransactionId(), false}, tuple);
        if (!new_rid.has_value()) {
          throw Exception(fmt::format("Insert Entry into table_heap error_1 {}", tuple.ToString(&schema)));
        }
        if (!MarkUndoVersionLink(exec_ctx_, new_rid.value())) {
          txn->SetTainted();
          throw ExecutionException(fmt::format("Other txn updating rid{}-{}, So txn{} failed", new_rid->GetPageId(),
                                             new_rid->GetSlotNum(), txn->GetTransactionIdHumanReadable()));
        }
        txn->AppendWriteSet(table_info_->oid_, new_rid.value());
        if (!primary_key_index_->index_->InsertEntry(key, new_rid.value(), txn)) {
          txn->SetTainted();
          throw ExecutionException("Insert Entry into primary_index failed2");
        }
        UnmarkUndoVersionLink(exec_ctx_, new_rid.value());
      } else {
        RID rid = result[0];
        auto [origin_meta, origin_tuple] = heap_->GetTuple(rid);

        if (!origin_meta.is_deleted_) {
          txn->SetTainted();
          throw ExecutionException(fmt::format("Rid{}-{} should be deleted, but not. So txn{} failed", rid.GetPageId(),
                                             rid.GetSlotNum(), txn->GetTransactionIdHumanReadable()));
        }

        if (origin_meta.ts_ > txn->GetReadTs() && origin_meta.ts_ != txn->GetTransactionTempTs()) {
          txn->SetTainted();
          throw ExecutionException(fmt::format("update_executor failed for {} in txn{}, read_ts{}", origin_meta.ts_,
                                            txn->GetTransactionIdHumanReadable(), txn->GetReadTs()));
        }

        if (!MarkUndoVersionLink(exec_ctx_, rid)) {
          txn->SetTainted();
          throw ExecutionException("Write-write confilct at update_executor");
        }
        txn->AppendWriteSet(table_info_->oid_, rid);
        
        if (origin_meta.ts_ == txn->GetTransactionTempTs()) {
          // 本事务删的
          // 需要维护undo_log，这个undo_log肯定是本事务之前维护过的
          // 如果没有undo_log，说明可能是本事务新插入的，因此也不需要维护undo_log
          auto undo_link = txn_mgr->GetUndoLink(rid);
          if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_log_idx_ != -1) {
            BUSTUB_ENSURE(undo_link->prev_txn_ == txn->GetTransactionId(), "Something wrong");
            auto undo_log = txn_mgr->GetUndoLog(undo_link.value());
            auto old_tuple = ReconstructTuple(&schema, origin_tuple, origin_meta,{undo_log});
            if (old_tuple.has_value()) {
              auto new_undo_log = GenerateUndoLog(tuple, old_tuple.value(), &schema, undo_log.ts_, undo_log.prev_version_);
              txn->ModifyUndoLog(undo_link->prev_log_idx_, new_undo_log);
            } else {
              assert(false);
            }
          }
        } else {
          // 本事务开启前就被删了
          // 相当于新插入，所以不需要插入和更新undo_log, 也不需要维护primary_index
          //auto ver_link = txn_mgr->GetVersionLink(rid);
          txn_mgr->UpdateUndoLink(rid, std::nullopt);
          //BUSTUB_ASSERT(!ver_link.has_value(), "ver_link must be std::nullopt");
        }
        CheckUpdateObject ch(origin_meta, origin_tuple, rid);

        if (!heap_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, tuple, rid, ch)) {
          txn->SetTainted();
          UnmarkUndoVersionLink(exec_ctx_, rid);
          throw ExecutionException(fmt::format("update_executor failed UpdateTupleInPlace for {} in txn{}, read_ts{}", origin_meta.ts_,
                                              txn->GetTransactionIdHumanReadable(), txn->GetReadTs()));            
        }
        heap_->UpdateTupleInPlace({txn->GetTransactionTempTs(), false}, tuple, rid);
        UnmarkUndoVersionLink(exec_ctx_, rid);
      }

    }
  }

  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(count));
  *tuple = Tuple(values, &GetOutputSchema());
  *rid = RID{};
  return true;
}
}  // namespace bustub
