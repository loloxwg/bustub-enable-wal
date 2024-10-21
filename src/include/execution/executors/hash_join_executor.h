//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <queue>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

class HashTable {
  public:
    HashTable(AbstractExecutor *left, AbstractExecutor *right) {

    }

};

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto GetLeftJoinKey(Tuple *tuple) -> HashJoinKey {
    std::vector<Value> vals;
    for (const auto &expr : plan_->left_key_expressions_) {
      auto val = expr->Evaluate(tuple, left_child_->GetOutputSchema());
      vals.emplace_back(val);
    }
    return {vals};
  }

  auto GetRightJoinKey(Tuple *tuple) -> HashJoinKey {
    std::vector<Value> vals;
    for (const auto &expr : plan_->right_key_expressions_) {
      auto val = expr->Evaluate(tuple, right_child_->GetOutputSchema());
      vals.emplace_back(val);
    }
    return {vals};
  }

  auto GetOutputTuple(Tuple *left_tuple, Tuple *right_tuple) -> Tuple {
    std::vector<Value> vals;
    auto left_schema = plan_->GetLeftPlan()->OutputSchema();
    for (size_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
      vals.emplace_back(left_tuple->GetValue(&left_schema, idx));
    }
    auto right_schema = plan_->GetRightPlan()->OutputSchema();
    for (size_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
      vals.emplace_back(right_tuple->GetValue(&right_schema, idx));
    }
    return {vals, &GetOutputSchema()};
  }

  auto GetLeftOutputTuple(Tuple *left_tuple) -> Tuple {
     std::vector<Value> vals;
    auto left_schema = plan_->GetLeftPlan()->OutputSchema();
    for (size_t idx = 0; idx < left_schema.GetColumnCount(); idx++) {
      vals.emplace_back(left_tuple->GetValue(&left_schema, idx));
    }
    auto right_schema = plan_->GetRightPlan()->OutputSchema();
    for (size_t idx = 0; idx < right_schema.GetColumnCount(); idx++) {
      vals.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
    }
    return {vals, &GetOutputSchema()};
  }

  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unordered_map<HashJoinKey, std::vector<Tuple>> hash_table_;
  std::vector<Tuple> *right_tuple_vec_;
  Tuple left_tuple_;
  size_t right_offset_;
};

}  // namespace bustub
