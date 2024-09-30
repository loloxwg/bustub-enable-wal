//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <climits>
#include <cstddef>
#include <iterator>
#include <mutex>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "fmt/core.h"
#include "fmt/format.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock_guard(latch_);

  size_t last_access = SIZE_MAX;
  auto found = false;
  std::list<LRUKNode>::iterator deleted_iter;

  for (auto iter = history_list_.begin(); iter != history_list_.end(); iter++) {
    LRUKNode &node = *iter;
    if (node.is_evictable_ && node.FirstAccess() < last_access) {
      *frame_id = node.fid_;
      last_access = node.FirstAccess();
      found = true;
      deleted_iter = iter;
    }
  }
  if (found) {
    history_list_.erase(deleted_iter);
    node_store_.erase(*frame_id);
    curr_size_--;
    return true;
  }

  for (auto iter = cache_list_.begin(); iter != cache_list_.end(); iter++) {
    LRUKNode &node = *iter;
    if (node.is_evictable_ && node.FirstAccess() < last_access) {
      *frame_id = node.fid_;
      last_access = node.FirstAccess();
      found = true;
      deleted_iter = iter;
    }
  }

  if (found) {
    cache_list_.erase(deleted_iter);
    node_store_.erase(*frame_id);
    curr_size_--;
  }
  return found;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  BUSTUB_ENSURE(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_),
                fmt::format("frame_id {} is not in range({}, {})", frame_id, 0, replacer_size_));
  std::lock_guard<std::mutex> lock_guard(latch_);

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    LRUKNode node{};
    node.k_ = k_;
    node.fid_ = frame_id;
    auto reach_k = node.Access(current_timestamp_);
    if (reach_k) {
      cache_list_.push_back(node);
      node_store_.emplace(frame_id, std::prev(cache_list_.end()));
    } else {
      history_list_.push_back(node);
      node_store_.emplace(frame_id, std::prev(history_list_.end()));
    }
  } else {
    LRUKNode &node = *iter->second;
    auto reach_k = node.Access(current_timestamp_);
    if (reach_k) {
      cache_list_.splice(cache_list_.end(), history_list_, iter->second);
      node_store_[frame_id] = std::prev(cache_list_.end());
    }
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ENSURE(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_),
                fmt::format("frame_id {} is not in range({}, {})", frame_id, 0, replacer_size_));
  std::lock_guard<std::mutex> lock_guard(latch_);

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }

  LRUKNode &node = *iter->second;
  if (node.is_evictable_ == set_evictable) {
    return;
  }
  node.is_evictable_ = set_evictable;
  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ENSURE(frame_id >= 0 && frame_id < static_cast<frame_id_t>(replacer_size_),
                fmt::format("frame_id {} is not in range({}, {})", frame_id, 0, replacer_size_));
  std::lock_guard<std::mutex> lock_guard(latch_);

  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) {
    return;
  }

  LRUKNode &node = *iter->second;
  if (!node.is_evictable_) {
    throw bustub::Exception(fmt::format("remove not evicatable page {}", frame_id));
  }

  if (node.history_.size() == k_) {
    cache_list_.erase(iter->second);
  } else {
    history_list_.erase(iter->second);
  }
  node_store_.erase(iter);
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock_guard(latch_);
  return curr_size_;
}

}  // namespace bustub
