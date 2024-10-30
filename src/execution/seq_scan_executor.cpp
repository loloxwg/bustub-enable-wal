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
  while (cursor_ < rids_.size()) {
    auto [meta, tuple_] = table_heap_->GetTuple(rids_[cursor_]);
    if (!meta.is_deleted_) {
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
    }
    cursor_++;
  }
  return false;
}

}  // namespace bustub
