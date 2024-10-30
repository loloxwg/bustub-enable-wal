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
  *tuple = found_tuple;
  *rid = result[0];
  return true;
}
}  // namespace bustub
