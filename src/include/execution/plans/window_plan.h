//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_plan.h
//
// Identification: src/include/execution/plans/aggregation_plan.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <_types/_uint32_t.h>
#include <memory>
#include <string>
#include <map>
#include <unordered_map>
#include <utility>
#include <vector>

#include "binder/bound_order_by.h"
#include "catalog/column.h"
#include "common/util/hash_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/aggregation_plan.h"
#include "fmt/format.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

/** WindowFunctionType enumerates all the possible window functions in our system */
enum class WindowFunctionType { CountStarAggregate, CountAggregate, SumAggregate, MinAggregate, MaxAggregate, Rank };

class WindowFunctionPlanNode : public AbstractPlanNode {
 public:
  /**
   * Construct a new WindowFunctionPlanNode.
   * @param output_schema The output format of this plan node
   * @param child The child plan to aggregate data over
   * @param window_func_indexes The indexes of the window functions
   * @param columns All columns include the placeholder for window functions
   * @param partition_bys The partition by clause of the window functions
   * @param order_bys The order by clause of the window functions
   * @param funcions The expressions that we are aggregating
   * @param window_func_types The types that we are aggregating
   *
   * Window Aggregation is different from normal aggregation as it outputs one row for each inputing rows,
   * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
   * normal selected columns and placeholder columns for window aggregations.
   *
   * For example, if we have a query like:
   *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY
   * 0.2,0.3) FROM table;
   *
   * The WindowFunctionPlanNode should contains following structure:
   *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
   *    partition_bys: std::vector<std::vector<AbstractExpressionRef>>{{0.2}, {0.1}}
   *    order_bys: std::vector<std::vector<AbstractExpressionRef>>{{0.3}, {0.2,0.3}}
   *    functions: std::vector<AbstractExpressionRef>{0.3, 0.4}
   *    window_func_types: std::vector<WindowFunctionType>{SumAggregate, SumAggregate}
   */
  WindowFunctionPlanNode(SchemaRef output_schema, AbstractPlanNodeRef child, std::vector<uint32_t> window_func_indexes,
                         std::vector<AbstractExpressionRef> columns,
                         std::vector<std::vector<AbstractExpressionRef>> partition_bys,
                         std::vector<std::vector<std::pair<OrderByType, AbstractExpressionRef>>> order_bys,
                         std::vector<AbstractExpressionRef> functions,
                         std::vector<WindowFunctionType> window_func_types)
      : AbstractPlanNode(std::move(output_schema), {std::move(child)}), columns_(std::move(columns)) {
    for (uint32_t i = 0; i < window_func_indexes.size(); i++) {
      window_functions_[window_func_indexes[i]] =
          WindowFunction{functions[i], window_func_types[i], partition_bys[i], order_bys[i]};
    }
  }

  /** @return The type of the plan node */
  auto GetType() const -> PlanType override { return PlanType::Window; }

  /** @return the child of this aggregation plan node */
  auto GetChildPlan() const -> AbstractPlanNodeRef {
    BUSTUB_ASSERT(GetChildren().size() == 1, "Window Aggregation expected to only have one child.");
    return GetChildAt(0);
  }

  static auto InferWindowSchema(const std::vector<AbstractExpressionRef> &columns) -> Schema;

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(WindowFunctionPlanNode);

  struct WindowFunction {
    AbstractExpressionRef function_;
    WindowFunctionType type_;
    std::vector<AbstractExpressionRef> partition_by_;
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_by_;
  };

  /** all columns expressions */
  std::vector<AbstractExpressionRef> columns_;

  std::unordered_map<uint32_t, WindowFunction> window_functions_;

 protected:
  auto PlanNodeToString() const -> std::string override;
};

class SimpleWindowHashTable {
 public:
  /**
   * Construct a new SimpleAggregationHashTable instance.
   * @param agg_exprs the aggregation expressions
   * @param agg_types the types of aggregations
   */
  explicit SimpleWindowHashTable(const WindowFunctionType &agg_type)
      : agg_type_{agg_type} {}

  SimpleWindowHashTable(const SimpleWindowHashTable& other) = default;      

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
  void CombineAggregateValues(Value *result, const Value &input) {
    switch (agg_type_) {
      case WindowFunctionType::CountStarAggregate:
        result->Add(ValueFactory::GetIntegerValue(1));
        break;
      case WindowFunctionType::CountAggregate:
        if (input.IsNull()) {
          return;
        }
        if (result->IsNull()) {
          *result = ValueFactory::GetIntegerValue(1);
        } else {
          *result = result->Add(ValueFactory::GetIntegerValue(1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (input.IsNull()) {
          return;
        }
        if (result->IsNull()) {
          *result = input;
        } else {
          *result = result->Add(input);
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (input.IsNull()) {
          return;
        }
        if (result->IsNull()) {
          *result = input;
        } else {
          *result = result->Min(input);
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (input.IsNull()) {
          return;
        }
        if (result->IsNull()) {
          *result = input;
        } else {
          *result = result->Max(input);
        }
        break;
      case WindowFunctionType::Rank:
        *result = result->Add(ValueFactory::GetIntegerValue(1));
        break;  
    }
  }

  /**
   * Inserts a value into the hash table and then combines it with the current aggregation.
   * @param agg_key the key to be inserted
   * @param agg_val the value to be inserted
   */
  auto InsertCombine(const AggregateKey &agg_key, const Value &agg_val) -> Value {
    if (ht_.count(agg_key) == 0) {
      ht_.insert({agg_key, GenerateInitialAggregateValue()});
    }
  
    CombineAggregateValues(&ht_[agg_key], agg_val);
    return ht_[agg_key];
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }
  
  private:
  /** The hash table is just a map from aggregate keys to aggregate values */
  std::unordered_map<AggregateKey, Value> ht_{};
  /** The aggregate expressions that we have */
  //const AbstractExpressionRef &agg_expr_;
  /** The types of aggregations that we have */
  const WindowFunctionType agg_type_{};
};

}  // namespace bustub

template <>
struct fmt::formatter<bustub::WindowFunctionPlanNode::WindowFunction> : formatter<std::string> {
  template <typename FormatContext>
  auto format(const bustub::WindowFunctionPlanNode::WindowFunction &x, FormatContext &ctx) const {
    return formatter<std::string>::format(fmt::format("{{ function_arg={}, type={}, partition_by={}, order_by={} }}",
                                                      x.function_, x.type_, x.partition_by_, x.order_by_),
                                          ctx);
  }
};

template <>
struct fmt::formatter<bustub::WindowFunctionType> : formatter<std::string> {
  template <typename FormatContext>
  auto format(bustub::WindowFunctionType c, FormatContext &ctx) const {
    using bustub::WindowFunctionType;
    std::string name = "unknown";
    switch (c) {
      case WindowFunctionType::CountStarAggregate:
        name = "count_star";
        break;
      case WindowFunctionType::CountAggregate:
        name = "count";
        break;
      case WindowFunctionType::SumAggregate:
        name = "sum";
        break;
      case WindowFunctionType::MinAggregate:
        name = "min";
        break;
      case WindowFunctionType::MaxAggregate:
        name = "max";
        break;
      case WindowFunctionType::Rank:
        name = "rank";
        break;
    }
    return formatter<std::string>::format(name, ctx);
  }
};
