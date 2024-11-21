#pragma once

#include <optional>
#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executor_context.h"
#include "storage/table/tuple.h"

namespace bustub {

class CheckInProcessObj {
 public:
  explicit CheckInProcessObj(VersionUndoLink vul) : vul_(vul) {}

  auto operator()(std::optional<VersionUndoLink> vul) -> bool {
    if (!vul.has_value()) {
      return false;
    }
    if (vul_ != vul.value()) {
      return false;
    }
    if (vul->in_progress_) {
      return false;
    }
    return true;
  }

 private:
  VersionUndoLink vul_;
};

auto MarkUndoVersionLink(ExecutorContext *exec_ctx, RID rid) -> bool;
void UnmarkUndoVersionLink(ExecutorContext *exec_ctx, RID rid);

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

auto GenerateUndoLog(const Tuple &new_tuple, const Tuple &tuple, const Schema *schema, timestamp_t ts,
                     std::optional<UndoLink> prev_link) -> UndoLog;

auto GenerateUndoLog(const Tuple &new_tuple, const Tuple &tuple, const UndoLog &old_log, const Schema *schema,
                     timestamp_t ts, std::optional<UndoLink> prev_link) -> UndoLog;

auto CheckModifyPrimaryKey(Tuple &old_tuple, Tuple &new_tuple, const IndexInfo *primary_key_index,
                           const TableInfo *table_info_) -> bool;

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
