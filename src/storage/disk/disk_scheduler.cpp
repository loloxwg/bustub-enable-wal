//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <chrono>
#include <memory>
#include <optional>
#include "common/channel.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/util/string_util.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // Spawn the background thread
  for (size_t i = 0; i < THREAD_NUMS; i++) {
    background_threads_.emplace_back([&] { StartWorkerThread(); });
  }
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  for (size_t i = 0; i < THREAD_NUMS; i++) {
    request_queue_.Put(std::nullopt);
  }
  for (size_t i = 0; i < THREAD_NUMS; i++) {
    background_threads_[i].join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::move(r)); }

void DiskScheduler::StartWorkerThread() {
  while (true) {
    auto req = request_queue_.Get();
    if (!req.has_value()) {
      return;
    }
    if (req->is_write_) {
      disk_manager_->WritePage(req->page_id_, req->data_);
      // std::cout << "write " << std::string(req->data_, 100) << std::endl;
      req->callback_.set_value(true);
    } else {
      disk_manager_->ReadPage(req->page_id_, req->data_);
      // std::cout << "read " << std::string(req->data_, 100) << std::endl;
      req->callback_.set_value(true);
    }
  }
}

}  // namespace bustub
