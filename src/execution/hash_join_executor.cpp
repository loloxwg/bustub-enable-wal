//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <cstddef>
#include <utility>
#include <vector>

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
}

void HashJoinExecutor::Init() { 
  left_child_->Init();
  right_child_->Init();
  Tuple tuple;
  RID rid;
  hash_table_.clear();
  while (right_child_->Next(&tuple, &rid)) {
    HashJoinKey key = GetRightJoinKey(&tuple);
    if (hash_table_.count(key) == 0) {
      hash_table_.insert({key, {tuple}});
    } else {
      hash_table_[key].emplace_back(tuple);
    }
  }
  right_offset_ = 0;
  right_tuple_vec_ = nullptr;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (right_tuple_vec_ != nullptr && right_offset_ < right_tuple_vec_->size()) {
    *tuple = GetOutputTuple(&left_tuple_, &((*right_tuple_vec_)[right_offset_]));
    *rid = RID{0}; 
    right_offset_++;
    return true;
  }

  RID right_rid;
  while (left_child_->Next(&left_tuple_, &right_rid)) {
    auto left_key = GetLeftJoinKey(&left_tuple_);
    if (hash_table_.count(left_key) != 0) {
      right_tuple_vec_ = &hash_table_[left_key];
      right_offset_ = 0;
      for (auto &it : *right_tuple_vec_) {
        right_offset_++;
        if (left_key == GetRightJoinKey(&it)) {
          *tuple = GetOutputTuple(&left_tuple_, &it);
          *rid = RID{0}; 
          return true;
        } 
      }
    } else if (plan_->join_type_ == JoinType::LEFT) {
      *tuple = GetLeftOutputTuple(&left_tuple_);
      *rid = RID{0}; 
      return true;
    }
  }
  return false;
}
}  // namespace bustub
