//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class SimpleWindowHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  explicit SimpleWindowHashTable(const WindowFunctionType &agg_type) : agg_type_{agg_type} {}

  SimpleWindowHashTable(const SimpleWindowHashTable &other) = default;

  /** @return The initial aggregate value for this aggregation executor */
  auto GenerateInitialAggregateValue() -> Value {
    switch (agg_type_) {
      case WindowFunctionType::CountStarAggregate:
        // Count start starts at zero.
        return ValueFactory::GetIntegerValue(0);
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        // Others starts at null.
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
      case WindowFunctionType::Rank:
        return ValueFactory::GetIntegerValue(0);
    }

    return {};
  }

  /**
   * TODO(Student)
   *
   * Combines the input into the aggregation result.
   * @param[out] result The output aggregate value
   * @param input The input value
   */
  void CombineAggregateValues(Value *result, const std::vector<Value> &input) {
    switch (agg_type_) {
      case WindowFunctionType::CountStarAggregate:
        *result = result->Add(ValueFactory::GetIntegerValue(1));
        break;
      case WindowFunctionType::CountAggregate:
        if (input[0].IsNull()) {
          return;
        }
        if (result->IsNull()) {
          *result = ValueFactory::GetIntegerValue(1);
        } else {
          *result = result->Add(ValueFactory::GetIntegerValue(1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (input[0].IsNull()) {
          return;
        }
        if (result->IsNull()) {
          *result = input[0];
        } else {
          *result = result->Add(input[0]);
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (input[0].IsNull()) {
          return;
        }
        if (result->IsNull()) {
          *result = input[0];
        } else {
          *result = result->Min(input[0]);
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (input[0].IsNull()) {
          return;
        }
        if (result->IsNull()) {
          *result = input[0];
        } else {
          *result = result->Max(input[0]);
        }
        break;
      case WindowFunctionType::Rank:
        count_++;
        if (rank_before_.empty() || !RankEqual(input, rank_before_)) {
          *result = ValueFactory::GetIntegerValue(count_);
        }

        rank_before_ = input;
        break;
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  auto InsertCombine(const AggregateKey &agg_key, const std::vector<Value> &agg_val) -> Value {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }

    CombineAggregateValues(&ht_[agg_key], agg_val);
    return ht_[agg_key];
  }

  auto ResultValue(const AggregateKey &agg_key) -> Value { return ht_[agg_key]; }

  auto RankEqual(const std::vector<Value> &v1, const std::vector<Value> &v2) -> bool {
    for (uint32_t i = 0; i < v1.size(); i++) {
      if (v1[i].CompareEquals(v2[i]) == CmpBool::CmpFalse) {
        return false;
      }
    }
    return true;
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, Value> ht_{};
  /** The aggregate expressions that we have */
  // const AbstractExpressionRef &agg_expr_;
  /** The types of aggregations that we have */
  std::vector<Value> rank_before_;
  uint32_t count_{0};
  const WindowFunctionType agg_type_{};
};

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::vector<Tuple> tuples_;
  std::vector<RID> rids_;
  std::vector<uint32_t> pos_;
  std::map<uint32_t, std::shared_ptr<SimpleWindowHashTable>> window_tables_;

  std::vector<Tuple> result_tuples_;
  uint32_t offset_;
};
}  // namespace bustub
