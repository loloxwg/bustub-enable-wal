#include "binder/bound_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/expressions/comparison_expression.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeEliminateTrueFilter(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    if (seq_scan_plan.filter_predicate_ == nullptr) {
      return optimized_plan;
    }
    // BUSTUB_ASSERT(seq_scan_plan.filter_predicate_, "must have push down predicate");
    auto indexs = catalog_.GetTableIndexes(seq_scan_plan.table_name_);
    if (indexs.empty()) {
      return optimized_plan;
    }

    const auto predicate = seq_scan_plan.filter_predicate_;
    auto cmp_predicate = dynamic_cast<ComparisonExpression*>(predicate.get());
    auto schema = seq_scan_plan.output_schema_;
    
    if (cmp_predicate != nullptr && cmp_predicate->comp_type_ == ComparisonType::Equal) {
      auto left_expr = cmp_predicate->GetChildAt(0);
      auto right_expr = cmp_predicate->GetChildAt(1);
      if (dynamic_cast<ConstantValueExpression*>(left_expr.get()) != nullptr && dynamic_cast<ColumnValueExpression*>(right_expr.get()) != nullptr) {
        auto const_expr = dynamic_cast<ConstantValueExpression*>(left_expr.get());
        auto col_expr = dynamic_cast<ColumnValueExpression*>(right_expr.get());
        for (const auto &index : indexs) {
          if (index->index_->GetKeySchema()->GetColumns().size() > 1) {
            continue;
          }
          auto index_cols = index->index_->GetKeySchema()->GetColumns();
          std::string name = seq_scan_plan.table_name_ + "." + index_cols[0].GetName();
          if (col_expr->GetColIdx() == schema->GetColIdx(name)) {
            return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index->index_oid_, predicate, const_expr);
          }
        }
      }

      if (dynamic_cast<ConstantValueExpression*>(right_expr.get()) != nullptr && dynamic_cast<ColumnValueExpression*>(left_expr.get()) != nullptr) {
        auto const_expr = dynamic_cast<ConstantValueExpression*>(right_expr.get());
        auto col_expr = dynamic_cast<ColumnValueExpression*>(left_expr.get());
        for (const auto &index : indexs) {
          if (index->index_->GetKeySchema()->GetColumns().size() > 1) {
            continue;
          }
          auto index_cols = index->index_->GetKeySchema()->GetColumns();
          std::string name = seq_scan_plan.table_name_ + "." + index_cols[0].GetName();
          if (col_expr->GetColIdx() == schema->GetColIdx(name)) {
            return std::make_shared<IndexScanPlanNode>(seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index->index_oid_, predicate, const_expr);
          }
        }
      }
    }
  }

  return plan;
}

}  // namespace bustub
