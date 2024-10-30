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

using hash_t = std::size_t;

class MyHashUtil {
 private:
  static const hash_t PRIME_FACTOR = 10000019;

 public:
  static inline auto HashBytes(const char *bytes, size_t length) -> hash_t {
    // https://github.com/greenplum-db/gpos/blob/b53c1acd6285de94044ff91fbee91589543feba1/libgpos/src/utils.cpp#L126
    hash_t hash = length;
    for (size_t i = 0; i < length; ++i) {
      hash = ((hash << 5) ^ (hash >> 27)) ^ bytes[i];
    }
    return hash;
  }

  static inline auto CombineHashes(hash_t l, hash_t r) -> hash_t {
    hash_t both[2] = {};
    both[0] = l;
    both[1] = r;
    return HashBytes(reinterpret_cast<char *>(both), sizeof(hash_t) * 2);
  }

  static inline auto SumHashes(hash_t l, hash_t r) -> hash_t {
    return (l % PRIME_FACTOR + r % PRIME_FACTOR) % PRIME_FACTOR;
  }

  template <typename T>
  static inline auto Hash(const T *ptr) -> hash_t {
    return HashBytes(reinterpret_cast<const char *>(ptr), sizeof(T));
  }

  template <typename T>
  static inline auto HashPtr(const T *ptr) -> hash_t {
    return HashBytes(reinterpret_cast<const char *>(&ptr), sizeof(void *));
  }

  /** @return the hash of the value */
  static inline auto HashValue(const Value *val) -> hash_t {
    switch (val->GetTypeId()) {
      case TypeId::TINYINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int8_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::SMALLINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int16_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::INTEGER: {
        auto raw = static_cast<int64_t>(val->GetAs<int32_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::BIGINT: {
        auto raw = static_cast<int64_t>(val->GetAs<int64_t>());
        return Hash<int64_t>(&raw);
      }
      case TypeId::BOOLEAN: {
        auto raw = val->GetAs<bool>();
        return Hash<bool>(&raw);
      }
      case TypeId::DECIMAL: {
        auto raw = val->GetAs<double>();
        return Hash<double>(&raw);
      }
      case TypeId::VARCHAR: {
        auto raw = val->GetData();
        auto len = val->GetLength();
        return HashBytes(raw, len);
      }
      case TypeId::TIMESTAMP: {
        auto raw = val->GetAs<uint64_t>();
        return Hash<uint64_t>(&raw);
      }
      default: {
        UNIMPLEMENTED("Unsupported type.");
      }
    }
  }
};

struct HashJoinKey {
  std::vector<Value> vals_;

  auto operator==(const HashJoinKey &other) const -> bool {
    for (uint32_t i = 0; i < other.vals_.size(); i++) {
      if (vals_[i].CompareEquals(other.vals_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {

/** Implements std::hash on AggregateKey */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &hash_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : hash_key.vals_) {
      if (!key.IsNull()) {
        curr_hash = bustub::MyHashUtil::CombineHashes(curr_hash, bustub::MyHashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {
class HashTable {
 public:
  HashTable(AbstractExecutor *left, AbstractExecutor *right) {}
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
