#include "execution/executors/window_function_executor.h"
#include <_types/_uint32_t.h>
#include <algorithm>
#include <memory>
#include <vector>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/util/tuple_util.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {

    }

void WindowFunctionExecutor::Init() { 
  child_executor_->Init();
  Tuple tuple;
  RID rid;
  uint32_t idx = 0;
  offset_ = 0;
  tuples_.clear();
  rids_.clear();
  pos_.clear();
  
  // aht_->Clear();
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.emplace_back(tuple);
    rids_.emplace_back(rid);
    pos_.emplace_back(idx);
    idx++;
  }

  auto win_func = *plan_->window_functions_.begin();  
  if (!win_func.second.order_by_.empty()) {
    TupleUtil::Sort(tuples_, pos_, win_func.second.order_by_, child_executor_->GetOutputSchema());
  }

  bool has_orde_by = !win_func.second.order_by_.empty();

  if (!has_orde_by) {
    while (offset_ < pos_.size()) {
      Tuple child_tuple = tuples_[pos_[offset_]];
      for (const auto &win_func : plan_->window_functions_) {
        if (window_tables_.find(win_func.first) == window_tables_.end()) {
          window_tables_[win_func.first] = std::make_shared<SimpleWindowHashTable>(win_func.second.type_);
        }
        auto table = window_tables_[win_func.first];
        AggregateKey key;
        for (const auto &expr : win_func.second.partition_by_) {
          key.group_bys_.emplace_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
        }
        auto value = win_func.second.function_->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
        table->InsertCombine(key, {value});
      }
      offset_++;    
    }
  }
  offset_ = 0;

  while (offset_ < pos_.size()) {
    Tuple child_tuple = tuples_[pos_[offset_]];
    std::vector<Value> values;
    uint32_t col_count = child_executor_->GetOutputSchema().GetColumnCount();
    values.reserve(col_count);
    for (uint32_t idx = 0; idx < col_count; idx++) {
      for (auto &col : plan_->columns_) {
        if (dynamic_cast<ColumnValueExpression*>(col.get()) != nullptr) {
          auto col_expr = dynamic_cast<ColumnValueExpression*>(col.get());
          if (col_expr->GetColIdx() == idx) {
            values.emplace_back(child_tuple.GetValue(&child_executor_->GetOutputSchema(), idx));
          }
        }
      }
    }

    for (const auto &win_func : plan_->window_functions_) {
      if (window_tables_.find(win_func.first) == window_tables_.end()) {
        window_tables_[win_func.first] = std::make_shared<SimpleWindowHashTable>(win_func.second.type_);
      }
      auto table = window_tables_[win_func.first];
      AggregateKey key;
      for (const auto &expr : win_func.second.partition_by_) {
        key.group_bys_.emplace_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
      }

      if (!has_orde_by) {
        values.emplace_back(table->ResultValue(key));
      } else if (win_func.second.type_ == WindowFunctionType::Rank) {
        std::vector<Value> agg_vals;
        for (const auto& pair : win_func.second.order_by_) {
          agg_vals.emplace_back(pair.second->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
        }
        values.emplace_back(table->InsertCombine(key, agg_vals));
      } else {
        auto value = win_func.second.function_->Evaluate(&child_tuple, child_executor_->GetOutputSchema());
        values.emplace_back(table->InsertCombine(key, {value}));
      }
    }

    result_tuples_.emplace_back(Tuple(values, &GetOutputSchema()));
    offset_++;    
  }
  offset_ = 0;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (offset_ == pos_.size()) {
    return false;
  }
  *tuple = result_tuples_[offset_];
  *rid = rids_[pos_[offset_]];
  offset_++;
  return true; 
}
}  // namespace bustub
