//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// string_util.cpp
//
// Identification: src/common/util/string_util.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/util/tuple_util.h"

namespace bustub {

void TupleUtil::Sort(const std::vector<Tuple> &tuples, std::vector<uint32_t> &pos,
                     const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by_,
                     const Schema &schema) {
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
}  // namespace bustub
