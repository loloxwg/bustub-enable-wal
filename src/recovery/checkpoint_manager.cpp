//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// checkpoint_manager.cpp
//
// Identification: src/recovery/checkpoint_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "recovery/checkpoint_manager.h"

namespace bustub {

void CheckpointManager::BeginCheckpoint() {
  // Block all the transactions and ensure that both the WAL and all dirty buffer pool pages are persisted to disk,
  // creating a consistent checkpoint. Do NOT allow transactions to resume at the end of this method, resume them
  // in CheckpointManager::EndCheckpoint() instead. This is for grading purposes.
  // Lock all txns
  transaction_manager_->BlockAllTransactions();
  // Force flush log
  log_manager_->Flush(true);
  // Flush pages to db file
  buffer_pool_manager_->FlushAllPages();
}

void CheckpointManager::EndCheckpoint() {
  // Allow transactions to resume, completing the checkpoint.
  transaction_manager_->ResumeTransactions();
}

void CheckpointManager::RunCheckPointThread() {
  if (enable_checkpointing) {
    return;
  }

  enable_checkpointing = true;
  checkpoint_thread_ = new std::thread([&] {
    while (true) {
      std::this_thread::sleep_for(checkpoint_timeout);
      BeginCheckpoint();
      EndCheckpoint();
    }
  });
}

void CheckpointManager::StopFlushThread() {
  enable_checkpointing = false;
  checkpoint_thread_->join();
  delete checkpoint_thread_;
  checkpoint_thread_ = nullptr;
}

}  // namespace bustub
