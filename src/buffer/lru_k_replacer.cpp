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

  if (curr_size_ == 0) {
    return false;
  }

  for (auto iter = history_list_.begin(); iter != history_list_.end(); iter++) {
    if (iter->is_evictable_) {
      *frame_id = iter->fid_;
      node_store_.erase(iter->fid_);
      curr_size_--;
      history_list_.erase(iter);
      return true;
    }
  }
  for (auto iter = cache_list_.begin(); iter != cache_list_.end(); iter++) {
    if (iter->is_evictable_) {
      *frame_id = iter->fid_;
      node_store_.erase(iter->fid_);
      curr_size_--;
      cache_list_.erase(iter);
      return true;
    }
  }

  return false;
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
      if (access_type == AccessType::Scan) {
        history_list_.push_front(node);
        node_store_.emplace(frame_id, history_list_.begin());
      } else {
        history_list_.push_back(node);
        node_store_.emplace(frame_id, std::prev(history_list_.end()));
      }
    }
  } else {
    LRUKNode &node = *iter->second;
    auto reach_k = node.Access(current_timestamp_);
    if (reach_k) {
      std::list<LRUKNode>::iterator cache_iter;
      for (cache_iter = cache_list_.begin(); cache_iter != cache_list_.end(); ++cache_iter) {
        if (node < *cache_iter) {
          break;
        }
      }
      cache_list_.splice(cache_iter, history_list_, iter->second);
      node_store_[frame_id] = std::prev(cache_iter);
    } else if (node.history_.size() == k_) {
      // 重新把cache_list的node排列。
      LRUKNode new_node = *iter->second;
      cache_list_.erase(iter->second);

      auto insered = false;
      for (auto cache_iter = cache_list_.begin(); cache_iter != cache_list_.end(); ++cache_iter) {
        if (new_node < *cache_iter) {
          cache_list_.insert(cache_iter, new_node);
          node_store_[frame_id] = std::prev(cache_iter);
          insered = true;
          break;
        }
      }
      if (!insered) {
        cache_list_.insert(cache_list_.end(), new_node);
        node_store_[frame_id] = std::prev(cache_list_.end());
      }
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
