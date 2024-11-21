//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include <cassert>
#include <vector>
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  auto catalog = GetExecutorContext()->GetCatalog();
  table_heap_ = catalog->GetTable(plan_->table_oid_)->table_.get();
  index_info_ = catalog->GetIndex(plan_->GetIndexOid());
  called_ = false;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (called_) {
    return false;
  }
  called_ = true;

  auto htable = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());

  std::vector<RID> result;
  Tuple key({plan_->pred_key_->val_}, &index_info_->key_schema_);
  htable->ScanKey(key, &result, nullptr);
  if (result.empty()) {
    return false;
  }
  assert(result.size() == 1);

  auto [meta, found_tuple] = table_heap_->GetTuple(result[0]);
  auto txn_manager = GetExecutorContext()->GetTransactionManager();
  auto txn = GetExecutorContext()->GetTransaction();

  bool deteled = meta.is_deleted_;
  if (meta.ts_ > txn->GetReadTs() && meta.ts_ != txn->GetTransactionId()) {
    auto undo_link = txn_manager->GetUndoLink(result[0]);
    if (!undo_link.has_value() || !undo_link->IsValid() || undo_link->prev_log_idx_ == -1) {
      return false;
    }
    std::vector<UndoLog> undo_logs;
    auto undo_log = txn_manager->GetUndoLog(undo_link.value());
    undo_logs.emplace_back(undo_log);

    while (undo_log.ts_ > txn->GetReadTs() && undo_log.prev_version_.IsValid() && undo_log.prev_version_.prev_log_idx_ != -1) {
      undo_log = txn_manager->GetUndoLog(undo_log.prev_version_);
      undo_logs.emplace_back(undo_log);
    }
    if (undo_log.ts_ > txn->GetReadTs()) {
      return false;
    }
    auto result_tuple = ReconstructTuple(&GetOutputSchema(), found_tuple, meta, undo_logs);
    if (result_tuple.has_value()) {
      *tuple = result_tuple.value();
      *rid = result[0];
      return true;
    }
    return false;
  }

  if (deteled) {
    return false;
  }

  *tuple = found_tuple;
  *rid = result[0];
  return true;
}
}  // namespace bustub
