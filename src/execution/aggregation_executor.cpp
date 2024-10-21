//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "storage/table/tuple.h"
#include "type/integer_type.h"
#include "type/value_factory.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
    plan_ = plan;
    child_executor_ = std::move(child_executor);
    aht_ = std::make_unique<SimpleAggregationHashTable>(plan_->GetAggregates(), plan_->GetAggregateTypes());
  }

void AggregationExecutor::Init() {
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  aht_->Clear();
  while (child_executor_->Next(&tuple, &rid)) {
    AggregateValue agg_value;
    AggregateKey agg_key;
    for (const auto &expr : plan_->GetAggregates()) {
      auto value = expr->Evaluate(&tuple, child_executor_->GetOutputSchema());
      agg_value.aggregates_.emplace_back(value); 
    }
    for (const auto &expr : plan_->GetGroupBys()) {
      auto value = expr->Evaluate(&tuple, child_executor_->GetOutputSchema());
      agg_key.group_bys_.emplace_back(value);
    }
    aht_->InsertCombine(agg_key, agg_value);
    has_value_ = true;
  }
  aht_iterator_ = std::make_unique<SimpleAggregationHashTable::Iterator>(aht_->Begin());
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (!has_value_) {
    has_value_ = true;
    if (plan_->group_bys_.empty()) {
      *tuple = Tuple{aht_->GenerateInitialAggregateValue().aggregates_, &GetOutputSchema()};
      *rid = RID{0};
      return true;
    }
    return false;
  }
  if (*aht_iterator_ == aht_->End()) {
    return false;
  }

  auto key = aht_iterator_->Key();
  auto val = aht_iterator_->Val();
  std::vector<Value> values;
  values.reserve(key.group_bys_.size() + val.aggregates_.size());
  values.insert(values.end(), key.group_bys_.begin(), key.group_bys_.end());
  values.insert(values.end(), val.aggregates_.begin(), val.aggregates_.end());
  *tuple = Tuple{values, &GetOutputSchema()};
  *rid = RID{0};
  ++(*aht_iterator_);
  return true; 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
