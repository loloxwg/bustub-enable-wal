//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

class TupleUtil {
 public:
  static void Sort(const std::vector<Tuple> &tuples, std::vector<uint32_t> &pos,
                   const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by_, const Schema &schema) {
    std::sort(pos.begin(), pos.end(), [&](uint32_t idx1, uint32_t idx2) {
      for (const auto &pair : order_by_) {
        Value left = pair.second->Evaluate(&tuples[idx1], schema);
        Value right = pair.second->Evaluate(&tuples[idx2], schema);
        auto cmp = left.CompareEquals(right);
        if (cmp == CmpBool::CmpTrue) {
          continue;
        }
        if (pair.first == OrderByType::ASC || pair.first == OrderByType::DEFAULT) {
          return left.CompareLessThan(right) == CmpBool::CmpTrue;
        }
        return left.CompareGreaterThan(right) == CmpBool::CmpTrue;
      }
      return false;
    });
  }
};

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> tuples_;
  std::vector<RID> rids_;
  std::vector<uint32_t> pos_;
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
  size_t offset_;
};
}  // namespace bustub
