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
#include <optional>
#include "common/config.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) { request_queue_.Put(std::make_optional(std::move(r))); }

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
