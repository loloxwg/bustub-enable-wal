//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// string_util.h
//
// Identification: src/include/common/util/string_util.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include "binder/bound_order_by.h"
#include "catalog/schema.h"
#include "execution/expressions/abstract_expression.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * StringUtil provides INEFFICIENT utility functions for working with strings. They should only be used for debugging.
 */
class TupleUtil {
 public:
  static void Sort(const std::vector<Tuple> &tuples, std::vector<uint32_t> &pos,
                   const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by_, const Schema &schema);
};

}  // namespace bustub
