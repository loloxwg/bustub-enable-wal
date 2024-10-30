#include "execution/executors/sort_executor.h"
#include <algorithm>
#include <cstddef>
#include <utility>
#include <vector>
#include "binder/bound_order_by.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/type.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  order_bys_ = plan_->order_bys_;
}

void SortExecutor::Init() {
  Tuple tuple;
  RID rid;

  offset_ = 0;
  size_t idx = 0;
  child_executor_->Init();
  tuples_.clear();
  rids_.clear();
  pos_.clear();
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
    rids_.emplace_back(rid);
    pos_.push_back(idx);
    idx++;
  }

  TupleUtil::Sort(tuples_, pos_, order_bys_, child_executor_->GetOutputSchema());
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (offset_ == pos_.size()) {
    return false;
  }
  *tuple = tuples_[pos_[offset_]];
  *rid = rids_[pos_[offset_]];
  offset_++;
  return true;
}
}  // namespace bustub
