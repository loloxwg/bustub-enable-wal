#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

using ExprVec = std::vector<AbstractExpressionRef>;

static auto Visit(const AbstractExpressionRef &expr, ExprVec *expr1, ExprVec *expr2) -> bool {
  if (dynamic_cast<ComparisonExpression*>(expr.get()) != nullptr) {
    auto comp_expr = dynamic_cast<ComparisonExpression*>(expr.get());
    if (comp_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    auto left_expr = comp_expr->GetChildAt(0);
    auto right_expr = comp_expr->GetChildAt(1);
    if (dynamic_cast<ColumnValueExpression*>(left_expr.get()) != nullptr && dynamic_cast<ColumnValueExpression*>(right_expr.get()) != nullptr) {
        auto col_expr1 = dynamic_cast<ColumnValueExpression*>(left_expr.get());
        auto col_expr2 = dynamic_cast<ColumnValueExpression*>(right_expr.get());
        if (col_expr1->GetTupleIdx() == col_expr2->GetTupleIdx()) {
          return false;
        }
        if (col_expr1->GetTupleIdx() == 0) {
          expr1->emplace_back(left_expr);
          expr2->emplace_back(right_expr);
        } else {
          expr1->emplace_back(right_expr);
          expr2->emplace_back(left_expr);
        }
        return true;
    }
  } else if (dynamic_cast<LogicExpression*>(expr.get()) != nullptr) {
    auto logic_expr = dynamic_cast<LogicExpression*>(expr.get());
    if (logic_expr->logic_type_ != LogicType::And) {
      return false;
    }
    return Visit(logic_expr->GetChildAt(0), expr1, expr2) && Visit(logic_expr->GetChildAt(1), expr1, expr2);
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(optimized_plan->children_.size() == 2, "Join should with two children!");

    ExprVec left_vec;
    ExprVec right_vec;
    if (Visit(nlj_plan.predicate_, &left_vec, &right_vec)) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetChildAt(0), nlj_plan.GetChildAt(1), left_vec, right_vec, nlj_plan.join_type_);
    }
  }
  
  return optimized_plan;
}

}  // namespace bustub
