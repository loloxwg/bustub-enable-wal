//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <vector>
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "type/value_factory.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void SeqScanExecutor::Init() {
  auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();
  auto iter = table_heap_->MakeIterator();
  cursor_ = 0;
  rids_.clear();

  while (!iter.IsEnd()) {
    rids_.emplace_back(iter.GetRID());
    ++iter;
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto txn_manager = GetExecutorContext()->GetTransactionManager();
  auto txn = GetExecutorContext()->GetTransaction();

  while (cursor_ < rids_.size()) {
    auto [meta, tuple_] = table_heap_->GetTuple(rids_[cursor_]);
    bool deteled = meta.is_deleted_;
    if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionId()) {
      auto undo_link = txn_manager->GetUndoLink(rids_[cursor_]);
      if (!undo_link.has_value() || !undo_link->IsValid() || undo_link->prev_log_idx_ == -1) {
        cursor_++;
        continue;
      }
      std::vector<UndoLog> undo_logs;
      auto undo_log = txn_manager->GetUndoLog(undo_link.value());
      undo_logs.emplace_back(undo_log);

      while (undo_log.ts_ > txn->GetReadTs() && undo_log.prev_version_.IsValid() &&
             undo_log.prev_version_.prev_log_idx_ != -1) {
        undo_log = txn_manager->GetUndoLog(undo_log.prev_version_);
        undo_logs.emplace_back(undo_log);
      }
      if (undo_log.ts_ > txn->GetReadTs()) {
        cursor_++;
        continue;
      }
      auto result = ReconstructTuple(&GetOutputSchema(), tuple_, meta, undo_logs);
      if (result.has_value()) {
        tuple_ = result.value();
        deteled = false;
      } else {
        cursor_++;
        continue;
      }
    }

    if (deteled) {
      cursor_++;
      continue;
    }

    if (plan_->filter_predicate_ != nullptr) {
      if (plan_->filter_predicate_->Evaluate(&tuple_, GetOutputSchema())
              .CompareEquals(ValueFactory::GetBooleanValue(true)) == CmpBool::CmpTrue) {
        *tuple = tuple_;
        *rid = rids_[cursor_];
        cursor_++;
        return true;
      }
    } else {
      *tuple = tuple_;
      *rid = rids_[cursor_];
      cursor_++;
      return true;
    }

    cursor_++;
  }
  return false;
}

}  // namespace bustub
