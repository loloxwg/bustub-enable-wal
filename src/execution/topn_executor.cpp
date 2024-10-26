#include "execution/executors/topn_executor.h"
#include <cstddef>
#include <memory>
#include <queue>
#include <utility>
#include "type/type.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
    plan_ = plan;
    child_executor_ = std::move(child_executor);
    limit_ = plan->GetN();
    auto cmp = [&](const TopNItem &left, const TopNItem &right) {
      for (const auto &pair : plan_->order_bys_) {
        auto l_val = pair.second->Evaluate(&left.tuple_, child_executor_->GetOutputSchema());
        auto r_val = pair.second->Evaluate(&right.tuple_, child_executor_->GetOutputSchema());
        auto cmp = l_val.CompareEquals(r_val);
        if (cmp == CmpBool::CmpTrue) {
          continue;
        }
        if (pair.first == OrderByType::ASC || pair.first == OrderByType::DEFAULT) {
          return l_val.CompareLessThan(r_val) == CmpBool::CmpTrue;
        }
        return l_val.CompareGreaterThan(r_val) == CmpBool::CmpTrue;
      }
      return true;
    };
    queues_ = std::make_unique<std::priority_queue<TopNItem, std::vector<TopNItem>, CmpType>>(cmp);
  }

void TopNExecutor::Init() { 
  child_executor_->Init();
  while (!queues_->empty()) {
    queues_->pop();
  }
  items_.clear();

  Tuple tuple;
  RID rid;

  while (child_executor_->Next(&tuple, &rid)) {
    queues_->push({tuple, rid});
    if (queues_->size() > limit_) {
      queues_->pop();
    }
  }
  while (!queues_->empty()) {
    items_.emplace_back(queues_->top());
    queues_->pop();
  }
  offset_ = items_.size() - 1;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (offset_ < 0) {
    return false; 
  }
  TopNItem item = items_[offset_];
  *tuple = item.tuple_;
  *rid = item.rid_;
  offset_--;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { 
  if (offset_ <= 0) {
    return 0;
  }
  // std::cout << "offset " << offset_ << std::endl;
  return offset_ + 1;
};

}  // namespace bustub
