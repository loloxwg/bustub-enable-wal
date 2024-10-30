//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "fmt/ranges.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  has_value_ = left_executor_->Next(&left_tuple_, &left_rid_);
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!has_value_) {
    return false;
  }

  Tuple right_tuple;
  RID right_rid;

  while (right_executor_->Next(&right_tuple, &right_rid)) {
    if (plan_->predicate_
            ->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                           plan_->GetRightPlan()->OutputSchema())
            .GetAs<bool>()) {
      std::vector<Value> values{};
      auto left_schema = plan_->GetLeftPlan()->OutputSchema();
      for (size_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
        values.emplace_back(left_tuple_.GetValue(&left_schema, idx));
      }
      auto right_schema = plan_->GetRightPlan()->OutputSchema();
      for (size_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
        values.emplace_back(right_tuple.GetValue(&right_schema, idx));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = RID{0};
      left_join_return_ = true;
      return true;
    }
  }

  right_executor_->Init();
  if (plan_->join_type_ == JoinType::LEFT) {
    if (!left_join_return_) {
      std::vector<Value> values{};
      auto left_schema = plan_->GetLeftPlan()->OutputSchema();
      for (size_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
        values.emplace_back(left_tuple_.GetValue(&left_schema, idx));
      }
      auto right_schema = plan_->GetRightPlan()->OutputSchema();
      for (size_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
        values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = RID{0};
      left_join_return_ = false;
      has_value_ = left_executor_->Next(&left_tuple_, &left_rid_);
      return true;
    }
  }

  while (left_executor_->Next(&left_tuple_, &left_rid_)) {
    left_join_return_ = false;
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if (plan_->predicate_
              ->EvaluateJoin(&left_tuple_, plan_->GetLeftPlan()->OutputSchema(), &right_tuple,
                             plan_->GetRightPlan()->OutputSchema())
              .GetAs<bool>()) {
        std::vector<Value> values{};
        auto left_schema = plan_->GetLeftPlan()->OutputSchema();
        for (size_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
          values.emplace_back(left_tuple_.GetValue(&left_schema, idx));
        }
        auto right_schema = plan_->GetRightPlan()->OutputSchema();
        for (size_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
          values.emplace_back(right_tuple.GetValue(&right_schema, idx));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        *rid = RID{0};
        left_join_return_ = true;
        return true;
      }
    }
    if (plan_->join_type_ == JoinType::LEFT) {
      if (!left_join_return_) {
        std::vector<Value> values{};
        auto left_schema = plan_->GetLeftPlan()->OutputSchema();
        for (size_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
          values.emplace_back(left_tuple_.GetValue(&left_schema, idx));
        }
        auto right_schema = plan_->GetRightPlan()->OutputSchema();
        for (size_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
          values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        *rid = RID{0};
        left_join_return_ = false;
        has_value_ = left_executor_->Next(&left_tuple_, &left_rid_);
        right_executor_->Init();
        return true;
      }
    }
    right_executor_->Init();
  }
  return false;
}

}  // namespace bustub
