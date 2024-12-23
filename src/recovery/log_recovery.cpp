//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// log_recovery.cpp
//
// Identification: src/recovery/log_recovery.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/log_recovery.h"

#include <catalog/catalog.h>

#include "storage/page/table_page.h"

namespace bustub {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data, LogRecord *log_record) const {
  // convert data to record and check header
  auto record = reinterpret_cast<const LogRecord *>(data);
  if (record->size_ <= 0 || data + record->size_ > log_buffer_ + LOG_BUFFER_SIZE) {
    return false;
  }

  // copy header
  memcpy(reinterpret_cast<char *>(log_record), data, LogRecord::HEADER_SIZE);

  // copy body
  int pos = LogRecord::HEADER_SIZE;
  switch (log_record->GetLogRecordType()) {
    case LogRecordType::INSERT:
      memcpy(&log_record->insert_rid_, data + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->insert_tuple_.DeserializeFrom(data + pos);
      break;

    case LogRecordType::MARKDELETE:
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      memcpy(&log_record->delete_rid_, data + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->delete_tuple_.DeserializeFrom(data + pos);
      break;

    case LogRecordType::UPDATE:
      memcpy(&log_record->update_rid_, data + pos, sizeof(RID));
      pos += sizeof(RID);
      log_record->old_tuple_.DeserializeFrom(data + pos);
      pos += 4 + log_record->old_tuple_.GetLength();
      log_record->new_tuple_.DeserializeFrom(data + pos);
      break;

    case LogRecordType::NEWPAGE:
      memcpy(&log_record->prev_page_id_, data + pos, sizeof(page_id_t));
      pos += sizeof(page_id_t);
      memcpy(&log_record->page_id_, data + pos, sizeof(page_id_t));
      break;

    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
      break;

    case LogRecordType::CREATETABLE:
      int table_name_size;
      memcpy(&table_name_size, data + pos, sizeof(int));
      pos += sizeof(int);
      log_record->table_name_.resize(table_name_size);
      memcpy(log_record->table_name_.data(), data + pos, table_name_size);
      pos += log_record->table_name_.size();
      int column_num;
      memcpy(&column_num, data + pos, sizeof(int));
      pos += sizeof(int);
      log_record->columns_.reserve(column_num);
      log_record->columns_.resize(column_num);
      for (int i = 0; i < column_num; i++) {
        log_record->columns_[i].DeserializeFrom(data + pos);
        pos += sizeof(Column);
      }
      break;

    default:
      return false;
  }

  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  assert(enable_logging == false);
  while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) {
    // offset of current log buffer
    size_t pos = 0;
    LogRecord log_record;

    // deserialize log entry to record
    while (DeserializeLogRecord(log_buffer_ + pos, &log_record)) {
      // update lsn mapping
      auto lsn = log_record.lsn_;
      lsn_mapping_[lsn] = offset_ + pos;

      // Add txn to ATT with status UNDO
      active_txn_[log_record.txn_id_] = lsn;
      pos += log_record.size_;

      // redo if page was not wirtten to disk when crash happened
      switch (log_record.log_record_type_) {
        case LogRecordType::INSERT: {
          auto page =
              reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.insert_rid_.GetPageId()));
          if (page->GetLSN() < lsn) {
            page->WLatch();
            page->InsertTuple(log_record.insert_tuple_, &log_record.insert_rid_, nullptr, nullptr, nullptr);
            page->WUnlatch();
          }

          buffer_pool_manager_->UnpinPage(page->GetPageId(), page->GetLSN() < lsn);
          break;
        }

        case LogRecordType::UPDATE: {
          auto page =
              reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.update_rid_.GetPageId()));
          if (page->GetLSN() < lsn) {
            page->WLatch();
            page->UpdateTuple(log_record.new_tuple_, &log_record.old_tuple_, log_record.update_rid_, nullptr, nullptr,
                              nullptr);
            page->WUnlatch();
          }

          buffer_pool_manager_->UnpinPage(page->GetPageId(), page->GetLSN() < lsn);
          break;
        }

        case LogRecordType::MARKDELETE:
        case LogRecordType::APPLYDELETE:
        case LogRecordType::ROLLBACKDELETE: {
          auto page =
              reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId()));
          if (page->GetLSN() < lsn) {
            page->WLatch();
            if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
              page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
            } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
              page->ApplyDelete(log_record.delete_rid_, nullptr, nullptr);
            } else {
              page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
            }
            page->WUnlatch();
          }

          buffer_pool_manager_->UnpinPage(page->GetPageId(), page->GetLSN() < lsn);
          break;
        }

        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
          active_txn_.erase(log_record.txn_id_);
          break;

        case LogRecordType::NEWPAGE: {
          auto page_id = log_record.page_id_;
          auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.page_id_));
          if (page->GetLSN() < lsn) {
            auto prev_page_id = log_record.prev_page_id_;
            page->WLatch();
            page->Init(page_id, BUSTUB_PAGE_SIZE, prev_page_id, nullptr, nullptr);
            page->WUnlatch();
            if (prev_page_id != INVALID_PAGE_ID) {
              auto prev_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_page_id));
              if (prev_page->GetNextPageId() != page_id) {
                prev_page->SetNextPageId(page_id);
                buffer_pool_manager_->UnpinPage(prev_page_id, true);
              } else {
                buffer_pool_manager_->UnpinPage(prev_page_id, false);
              }
            }
          }

          buffer_pool_manager_->UnpinPage(page_id, page->GetLSN() < lsn);
          break;
        }
        case LogRecordType::CREATETABLE: {
          catalog_->CreateTable(nullptr, log_record.table_name_, Schema(log_record.columns_));
          break;
        }

        default:
          break;
      }
    }

    offset_ += pos;
  }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  // rollback
  for (auto &txn : active_txn_) {
    auto lsn = txn.second;
    auto offset = lsn_mapping_[lsn];
    LogRecord log_record;
    memcpy(reinterpret_cast<char *>(&log_record), log_buffer_ + offset, LogRecord::HEADER_SIZE);
    // deserialize log entry to record
    if (!DeserializeLogRecord(log_buffer_ + offset, &log_record)) {
      continue;
    }
    // undo
    switch (log_record.GetLogRecordType()) {
      case LogRecordType::INSERT: {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.insert_rid_.GetPageId()));
        page->WLatch();
        page->ApplyDelete(log_record.insert_rid_, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
      }

      case LogRecordType::UPDATE: {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.update_rid_.GetPageId()));
        page->WLatch();
        page->UpdateTuple(log_record.old_tuple_, &log_record.new_tuple_, log_record.update_rid_, nullptr, nullptr,
                          nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
      }

      case LogRecordType::MARKDELETE:
      case LogRecordType::APPLYDELETE:
      case LogRecordType::ROLLBACKDELETE: {
        auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(log_record.delete_rid_.GetPageId()));
        page->WLatch();
        if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
          page->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
        } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
          page->InsertTuple(log_record.delete_tuple_, &log_record.delete_rid_, nullptr, nullptr, nullptr);
        } else {
          page->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
        }
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
        break;
      }

      default:
        break;
    }
  }

  active_txn_.clear();
  lsn_mapping_.clear();
}
}  // namespace bustub
