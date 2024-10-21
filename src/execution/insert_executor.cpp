//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <_types/_uint32_t.h>
#include <memory>
#include <vector>

#include "common/rid.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
    plan_ = plan;
    child_executor_ = std::move(child_executor);
    auto catalog = GetExecutorContext()->GetCatalog();
    table_info_ = catalog->GetTable(plan_->table_oid_);
    heap_ = table_info_->table_.get();
    indexs_ = catalog->GetTableIndexes(table_info_->name_);
  }

void InsertExecutor::Init() { 
  child_executor_->Init();
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
  if (called_) {
    return false;
  }
  called_ = true;
  Tuple insert_tuple;
  RID insert_rid;
  int count = 0;
  while (child_executor_->Next(&insert_tuple, &insert_rid)) {
    TupleMeta meta{0, false};
    auto result_rid = heap_->InsertTuple(meta, insert_tuple);
    if (!result_rid.has_value()) {
      break;
    }
    
    for (auto index : indexs_) {
      auto schema = index->index_->GetKeySchema();
      std::vector<uint32_t> col_idxs;
      for (const auto &col : schema->GetColumns()) {
        col_idxs.emplace_back(table_info_->schema_.GetColIdx(col.GetName()));
      }
      Tuple key = insert_tuple.KeyFromTuple(table_info_->schema_, *schema, col_idxs);
      index->index_->InsertEntry(key, result_rid.value(), nullptr);
    }
    count++;
  }

  std::vector<Value> values{};
  values.push_back(ValueFactory::GetIntegerValue(count));

  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
