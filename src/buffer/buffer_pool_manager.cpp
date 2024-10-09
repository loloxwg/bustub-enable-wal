//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <mutex>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "fmt/core.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  for (size_t i = 0; i < pool_size_; ++i) {
    page_latch_.emplace_back(std::make_shared<std::mutex>());
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  frame_id_t fid;
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    page_latch_[fid]->lock();
    *page_id = AllocatePage();
    page_table_.emplace(*page_id, fid);
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    Page *page = &pages_[fid];
    latch_.unlock();
    page->page_id_ = *page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    page_latch_[fid]->unlock();
    return page;
  }

  if (replacer_->Evict(&fid)) {
    page_id_t old_pid = -1;
    for (const auto iter : page_table_) {
      if (iter.second == fid) {
        old_pid = iter.first;
      }
    }
    if (old_pid == -1) {
      throw bustub::Exception(fmt::format("Some bugs for {}", fid));
    }
    page_latch_[fid]->lock();
    page_table_.erase(old_pid);
    *page_id = AllocatePage();
    page_table_.emplace(*page_id, fid);
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);

    Page *page = &pages_[fid];
    latch_.unlock();

    if (page->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, page->GetData(), old_pid, std::move(promise)});
      if (!future.get()) {
        throw bustub::Exception(fmt::format("Error for fetch page_id {}", old_pid));
      }
    }
    page->is_dirty_ = false;
    page->pin_count_ = 1;
    page->page_id_ = *page_id;
    page_latch_[fid]->unlock();
    return page;
  }

  latch_.unlock();
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType access_type) -> Page * {
  if (page_id == INVALID_PAGE_ID) {
    throw bustub::Exception("Invalid page_id");
  }

  latch_.lock();
  auto iter = page_table_.find(page_id);
  frame_id_t fid;
  if (iter != page_table_.end()) {
    fid = iter->second;
    page_latch_[fid]->lock();
    replacer_->RecordAccess(fid, access_type);
    replacer_->SetEvictable(fid, false);
    Page *page = &pages_[fid];
    page->pin_count_++;
    latch_.unlock();
    page_latch_[fid]->unlock();
    return page;
  }

  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    page_latch_[fid]->lock();
    page_table_.emplace(page_id, fid);
    replacer_->RecordAccess(fid, access_type);
    replacer_->SetEvictable(fid, false);
    latch_.unlock();
  } else if (replacer_->Evict(&fid)) {
    page_id_t old_pid = -1;
    for (const auto iter : page_table_) {
      if (iter.second == fid) {
        old_pid = iter.first;
      }
    }

    if (old_pid == -1) {
      throw bustub::Exception(fmt::format("Some bugs for {}", fid));
    }

    page_latch_[fid]->lock();
    page_table_.erase(old_pid);
    page_table_.emplace(page_id, fid);
    replacer_->RecordAccess(fid, access_type);
    replacer_->SetEvictable(fid, false);

    Page *page = &pages_[fid];
    latch_.unlock();

    if (page->page_id_ != old_pid) {
      throw bustub::Exception(fmt::format("Some bugs for {}", old_pid));
    }

    if (page->is_dirty_) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, page->GetData(), old_pid, std::move(promise)});
      if (!future.get()) {
        throw bustub::Exception(fmt::format("Error for fetch page_id {}", page_id));
      }
    }

    page->is_dirty_ = false;
    page->pin_count_ = 0;
    page->page_id_ = page_id;

    // page_latch_[fid]->unlock();
  } else {
    latch_.unlock();
    return nullptr;
  }
  // page_latch_[fid]->lock();
  Page *page = &pages_[fid];
  BUSTUB_ENSURE(page->pin_count_ == 0, "New page's pin_count shoule be zero");
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page->GetData(), page_id, std::move(promise)});
  if (!future.get()) {
    throw bustub::Exception(fmt::format("Error for fetch page_id {}", page_id));
  }
  page_latch_[fid]->unlock();
  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  frame_id_t fid = iter->second;
  page_latch_[fid]->lock();

  Page *page = &pages_[fid];
  if (is_dirty) {
    page->is_dirty_ = is_dirty;
  }
  if (page_id != page->page_id_) {
    throw bustub::Exception(fmt::format("Error page_id for FlushAllPages {}", page_id));
  }
  if (page->pin_count_ == 0) {
    latch_.unlock();
    page_latch_[fid]->unlock();
    return false;
  }

  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  latch_.unlock();
  page_latch_[fid]->unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  // page_table_.erase(iter);
  frame_id_t fid = iter->second;

  page_latch_[fid]->lock();
  latch_.unlock();

  Page *page = &pages_[fid];

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  if (page_id != page->page_id_) {
    throw bustub::Exception(fmt::format("Error page_id for FlushAllPages {}", page_id));
  }
  disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
  if (!future.get()) {
    throw bustub::Exception(fmt::format("Error for FlushPage {}", page_id));
  }
  page->is_dirty_ = false;
  replacer_->SetEvictable(fid, true);
  page_latch_[fid]->unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock_guard(latch_);

  for (const auto iter : page_table_) {
    page_id_t page_id = iter.first;
    frame_id_t fid = iter.second;
    // page_table_.erase(iter);

    // page_latch_[fid]->lock();
    Page *page = &pages_[fid];

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    if (page_id != page->page_id_) {
      throw bustub::Exception(fmt::format("Error page_id for FlushAllPages {}", page_id));
    }
    disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
    if (!future.get()) {
      throw bustub::Exception(fmt::format("Error for FlushAllPages {}", page_id));
    }
    page->is_dirty_ = false;
    replacer_->SetEvictable(fid, true);
    // page_latch_[fid]->unlock();
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  frame_id_t fid = iter->second;
  page_latch_[fid]->lock();
  Page *page = &pages_[fid];
  if (page->pin_count_ > 0) {
    page_latch_[fid]->unlock();
    latch_.unlock();
    return false;
  }

  page_table_.erase(iter);
  free_list_.push_back(fid);
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page_id);
  replacer_->SetEvictable(fid, true);
  replacer_->Remove(fid);
  latch_.unlock();
  page_latch_[fid]->unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
