//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "execution/executors/delete_executor.h"
#include "type/value_factory.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  auto catalog = GetExecutorContext()->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  table_heap_ = table_info_->table_.get();
  indexs_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() { child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (called_) {
    return false;
  }
  called_ = true;

  int count = 0;
  Tuple i_tuple;
  RID i_rid;
  while (child_executor_->Next(&i_tuple, &i_rid)) {
    TupleMeta meta{0, true};
    table_heap_->UpdateTupleMeta(meta, i_rid);
    for (auto index : indexs_) {
      auto schema = index->index_->GetKeySchema();
      std::vector<uint32_t> col_idxs;
      for (const auto &col : schema->GetColumns()) {
        col_idxs.emplace_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      Tuple key = i_tuple.KeyFromTuple(table_info_->schema_, *schema, col_idxs);
      index->index_->DeleteEntry(key, i_rid, nullptr);
    }
    count++;
  }
  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(count));
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
