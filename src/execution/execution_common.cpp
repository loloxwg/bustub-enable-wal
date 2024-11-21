#include "execution/execution_common.h"
#include <cstdio>
#include <functional>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto GenerateUndoLog(const Tuple &new_tuple, const Tuple &tuple, const UndoLog &old_log, const Schema *schema,
                     timestamp_t ts, std::optional<UndoLink> prev_link) -> UndoLog {
  std::vector<bool> modified_fields;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    modified_fields.emplace_back(false);
  }
  std::vector<Value> values;
  std::vector<Column> columns;
  std::vector<bool> last_modified_fields = old_log.modified_fields_;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    auto new_value = new_tuple.GetValue(schema, i);
    auto value = tuple.GetValue(schema, i);
    if (!new_value.CompareExactlyEquals(value) || last_modified_fields[i]) {
      modified_fields[i] = true;
      values.emplace_back(value);
      columns.emplace_back(schema->GetColumn(i));
    }
  }
  Schema sch(columns);
  Tuple t(values, &sch);
  if (prev_link.has_value()) {
    return UndoLog{false, modified_fields, t, ts, prev_link.value()};
  }
  return UndoLog{false, modified_fields, t, ts, {}};
}

auto GenerateUndoLog(const Tuple &new_tuple, const Tuple &tuple, const Schema *schema, timestamp_t ts,
                     std::optional<UndoLink> prev_link) -> UndoLog {
  std::vector<bool> modified_fields;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    modified_fields.emplace_back(false);
  }
  if (IsTupleContentEqual(new_tuple, tuple)) {
    if (prev_link.has_value()) {
      return UndoLog{false, modified_fields, {}, ts, prev_link.value()};
    }
    return UndoLog{false, modified_fields, {}, ts, {}};
  }
  std::vector<Value> values;
  std::vector<Column> columns;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    auto new_value = new_tuple.GetValue(schema, i);
    auto value = tuple.GetValue(schema, i);
    if (!new_value.CompareExactlyEquals(value)) {
      modified_fields[i] = true;
      values.emplace_back(value);
      columns.emplace_back(schema->GetColumn(i));
    }
  }
  Schema sch(columns);
  Tuple t(values, &sch);
  if (prev_link.has_value()) {
    return UndoLog{false, modified_fields, t, ts, prev_link.value()};
  }
  return UndoLog{false, modified_fields, t, ts, {}};
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (undo_logs.empty()) {
    return base_meta.is_deleted_ ? std::nullopt : std::make_optional(base_tuple);
  }
  auto sz = undo_logs.size();
  if (undo_logs[sz - 1].is_deleted_) {
    return std::nullopt;
  }

  std::vector<Value> values;
  for (uint32_t idx = 0; idx < schema->GetColumnCount(); idx++) {
    values.emplace_back(base_tuple.GetValue(schema, idx));
  }
  for (const auto &undo_log : undo_logs) {
    if (!undo_log.is_deleted_) {
      std::vector<Column> columns;
      for (uint32_t idx = 0; idx < schema->GetColumnCount(); idx++) {
        if (undo_log.modified_fields_[idx]) {
          columns.emplace_back(schema->GetColumns()[idx]);
        }
      }
      Schema s(columns);
      for (uint32_t idx = 0, i = 0; idx < schema->GetColumnCount(); idx++) {
        if (undo_log.modified_fields_[idx]) {
          values[idx] = undo_log.tuple_.GetValue(&s, i);
          i++;
        }
      }
    }
  }

  return Tuple{values, schema};
}

/**
 * 如果标记成功返回true，如果标记失败返回false，如果没有undolink也返回true
 * **/
auto MarkUndoVersionLink(ExecutorContext *exec_ctx, RID rid) -> bool {
  auto vul = VersionUndoLink::FromOptionalUndoLink(exec_ctx->GetTransactionManager()->GetUndoLink(rid));
  if (vul.has_value()) {
    CheckInProcessObj check_obj(vul.value());
    vul->in_progress_ = true;
    return exec_ctx->GetTransactionManager()->UpdateVersionLink(rid, vul, check_obj);
  }
  UndoLink undo_link;
  undo_link.prev_log_idx_ = -1;
  undo_link.prev_txn_ = exec_ctx->GetTransaction()->GetTransactionId();
  vul = VersionUndoLink{};
  vul->in_progress_ = true;
  vul->prev_ = undo_link;

  auto fun = [&vul](std::optional<VersionUndoLink> ver_link) -> bool {
    if (!ver_link.has_value()) {
      return true;
    }
    return vul == ver_link.value();
  };
  return exec_ctx->GetTransactionManager()->UpdateVersionLink(rid, vul, fun);
}

void UnmarkUndoVersionLink(ExecutorContext *exec_ctx, RID rid) {
  auto vul = VersionUndoLink::FromOptionalUndoLink(exec_ctx->GetTransactionManager()->GetUndoLink(rid));
  if (vul.has_value()) {
    exec_ctx->GetTransactionManager()->UpdateVersionLink(rid, vul, nullptr);
  }
}

auto CheckModifyPrimaryKey(Tuple &old_tuple, Tuple &new_tuple, const IndexInfo *primary_key_index,
                           const TableInfo *table_info_) -> bool {
  if (primary_key_index == nullptr) {
    return false;
  }
  auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, primary_key_index->key_schema_,
                                        primary_key_index->index_->GetKeyAttrs());
  auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, primary_key_index->key_schema_,
                                        primary_key_index->index_->GetKeyAttrs());
  return !IsTupleContentEqual(old_key, new_key);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // fmt::println(
  //     stderr,
  //     "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
  //     "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
  //     tasks.");

  auto iter = table_heap->MakeIterator();
  auto water_mark = txn_mgr->GetWatermark();
  std::cerr << "water_mark:" << water_mark << std::endl;
  for (; !iter.IsEnd(); ++iter) {
    auto rid = iter.GetRID();
    auto pair = iter.GetTuple();
    fmt::println(stderr, "RID={}/{} ts={} is_deleted={} tuple={}", rid.GetPageId(), rid.GetSlotNum(), pair.first.ts_,
                 pair.first.is_deleted_, pair.second.ToString(&table_info->schema_));
    auto undo_link = txn_mgr->GetUndoLink(rid);
    Tuple tuple = pair.second;
    bool has_reach_end = pair.first.ts_ <= water_mark;

    if (!has_reach_end && undo_link.has_value() && undo_link->IsValid()) {
      UndoLog undo_log;
      while (!has_reach_end) {
        if (!undo_link.has_value() || !undo_link->IsValid() || undo_link->prev_log_idx_ == -1) {
          break;
        }
        undo_log = txn_mgr->GetUndoLog(undo_link.value());
        auto prev_tuple = ReconstructTuple(&table_info->schema_, tuple, pair.first, {undo_log});
        has_reach_end = undo_log.ts_ <= water_mark;
        if (prev_tuple.has_value()) {
          tuple = prev_tuple.value();
          fmt::println(stderr, "  txn{}@{} {} ts={}", undo_link->prev_txn_ ^ TXN_START_ID, undo_link->prev_log_idx_,
                       tuple.ToString(&table_info->schema_), undo_log.ts_);
          if (undo_log.prev_version_.IsValid()) {
            undo_link = undo_log.prev_version_;
          } else {
            break;
          }
        } else {
          fmt::println(stderr, "  txn{}@{} deleted ts={}", undo_link->prev_txn_ ^ TXN_START_ID,
                       undo_link->prev_log_idx_, undo_log.ts_);
          if (undo_log.prev_version_.IsValid()) {
            undo_link = undo_log.prev_version_;
          } else {
            break;
          }
        }
      }
    }
  }
  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
