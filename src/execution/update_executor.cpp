//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "common/rid.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);

  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  auto catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  heap_ = table_info_->table_.get();
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (called_) {
    return false;
  }
  called_ = true;
  int count = 0;
  Tuple i_tuple;
  RID i_rid;
  while (child_executor_->Next(&i_tuple, &i_rid)) {
    TupleMeta meta{0, true};
    heap_->UpdateTupleMeta(meta, i_rid);

    std::vector<Value> values;
    for (const auto &expr : plan_->target_expressions_) {
      values.emplace_back(expr->Evaluate(&i_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(values, &child_executor_->GetOutputSchema());
    auto insert_rid = heap_->InsertTuple({0, false}, new_tuple);

    if (!insert_rid.has_value()) {
      break;
    }
    for (auto index : indexs_) {
      auto schema = index->index_->GetKeySchema();
      std::vector<uint32_t> col_idxs;
      for (const auto &col : schema->GetColumns()) {
        col_idxs.emplace_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      Tuple delete_key = i_tuple.KeyFromTuple(table_info_->schema_, *schema, col_idxs);
      Tuple insert_key = new_tuple.KeyFromTuple(table_info_->schema_, *schema, col_idxs);

      index->index_->DeleteEntry(delete_key, i_rid, nullptr);
      if (!index->index_->InsertEntry(insert_key, insert_rid.value(), nullptr)) {
        std::vector<Value> values{};
        values.push_back(ValueFactory::GetIntegerValue(count));
        *tuple = Tuple(values, &GetOutputSchema());
        return false;
      }
    }
    count++;
  }
  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}
}  // namespace bustub
