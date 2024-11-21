//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <memory>
#include <utility>
#include <vector>

#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  auto catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  table_heap_ = table_info_->table_.get();
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (called_) {
    return false;
  }
  called_ = true;

  int count = 0;
  Tuple deleted_tuple;
  RID deleted_rid;
  auto txn_mgr = GetExecutorContext()->GetTransactionManager();
  auto txn = GetExecutorContext()->GetTransaction();
  auto schema = child_executor_->GetOutputSchema();

  while (child_executor_->Next(&deleted_tuple, &deleted_rid)) {
    auto meta = table_heap_->GetTupleMeta(deleted_rid);

    // 删除的元组必须是本事务开始前已提交的或者本事务新插入的
    // - 删除本事务新插入的元组需要额外删除本事务添加的undo_log
    // - 删除本事务开始前已提交的元组则需要添加undo_log
    if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionTempTs()) {
      txn->SetTainted();
      throw ExecutionException(fmt::format("delete_executor failed for meta.ts: {} in txn: {}, read_ts: {}", meta.ts_,
                                           txn->GetTransactionId(), txn->GetReadTs()));
    }

    // 本事务插入 修改 删除，那就不应该有undo_log
    // 本事务修改的，再删除，那就只能有一个undo_log
    if (meta.ts_ == txn->GetTransactionTempTs()) {
      auto undo_link = txn_mgr->GetUndoLink(deleted_rid);
      if (undo_link.has_value() && undo_link->IsValid() && undo_link->prev_log_idx_ != -1 && undo_link->prev_txn_ == txn->GetTransactionId()) {
        UndoLog undo_log = txn->GetUndoLog(undo_link->prev_log_idx_);
        auto origin_tuple = ReconstructTuple(&schema, deleted_tuple, meta, {undo_log});
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
      undo_log.tuple_ = deleted_tuple;
      undo_log.ts_ = meta.ts_;

      auto undo_link = txn_mgr->GetUndoLink(deleted_rid);
      if (undo_link.has_value()) {
        undo_log.prev_version_ = undo_link.value();
      }
      txn_mgr->UpdateUndoLink(deleted_rid, txn->AppendUndoLog(undo_log));
    }

    txn->AppendWriteSet(table_info_->oid_, deleted_rid);
    meta.is_deleted_ = true;
    meta.ts_ = txn->GetTransactionTempTs();
    table_heap_->UpdateTupleMeta(meta, deleted_rid);
    count++;
  }
  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(count));
  *tuple = Tuple(values, &GetOutputSchema());
  *rid = RID{};
  return true;
}

}  // namespace bustub
