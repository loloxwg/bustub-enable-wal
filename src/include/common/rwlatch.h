//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwmutex.h
//
// Identification: src/include/common/rwlatch.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>  // NOLINT
#include <condition_variable>
#include <atomic>
#include <pthread.h>
#include <shared_mutex>

#include "common/macros.h"

// /*
//  * class ReaderWriterLatch {
//  public:
//   /**
//    * Acquire a write latch.
//    */
//   void WLock() { mutex_.lock(); }
//
//   /**
//    * Release a write latch.
//    */
//   void WUnlock() { mutex_.unlock(); }
//
//   /**
//    * Acquire a read latch.
//    */
//   void RLock() { mutex_.lock_shared(); }
//
//   /**
//    * Release a read latch.
//    */
//   void RUnlock() { mutex_.unlock_shared(); }
//
//  private:
//   std::shared_mutex mutex_;
// };**/

namespace bustub {

/**
 * 基于C++11标准库实现的读写锁
 * 特点：
 * 1. 使用原子操作优化性能
 * 2. 允许多个读者同时访问
 * 3. 写者需要独占访问
 * 4. 写者优先，防止写者饥饿
 */
class ReaderWriterLatch {
 public:
  ReaderWriterLatch() : reader_count_(0), writer_active_(false), writer_waiting_(0) {}

  /**
   * 获取写锁
   */
  void WLock() {
    // 增加等待写者计数，使用原子操作
    writer_waiting_.fetch_add(1, std::memory_order_acquire);

    std::unique_lock<std::mutex> lock(mutex_);
    // 等待直到没有活跃的读者和写者
    cond_.wait(lock, [this] {
      return reader_count_.load(std::memory_order_acquire) == 0 && !writer_active_.load(std::memory_order_acquire);
    });

    // 减少等待写者计数
    writer_waiting_.fetch_sub(1, std::memory_order_release);
    // 设置写者活跃标志
    writer_active_.store(true, std::memory_order_release);
  }

  /**
   * 释放写锁
   */
  void WUnlock() {
    // 清除写者活跃标志，使用原子操作
    writer_active_.store(false, std::memory_order_release);

    // 只在需要时获取锁并通知等待线程
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cond_.notify_all();
    }
  }

  /**
   * 获取读锁
   */
  void RLock() {
    // 快速路径：如果没有写者活跃或等待，直接增加读者计数
    if (!writer_active_.load(std::memory_order_acquire) &&
        writer_waiting_.load(std::memory_order_acquire) == 0) {
      reader_count_.fetch_add(1, std::memory_order_acq_rel);
      return;
    }

    // 慢路径：需要等待写者
    std::unique_lock<std::mutex> lock(mutex_);
    cond_.wait(lock, [this] {
      return !writer_active_.load(std::memory_order_acquire) &&
             writer_waiting_.load(std::memory_order_acquire) == 0;
    });

    // 增加读者计数
    reader_count_.fetch_add(1, std::memory_order_acq_rel);
  }

  /**
   * 释放读锁
   */
  void RUnlock() {
    // 减少读者计数，使用原子操作
    int prev_readers = reader_count_.fetch_sub(1, std::memory_order_acq_rel);

    // 如果是最后一个读者，且有写者等待，通知写者
    if (prev_readers == 1 &&
        (writer_active_.load(std::memory_order_acquire) ||
         writer_waiting_.load(std::memory_order_acquire) > 0)) {
      std::unique_lock<std::mutex> lock(mutex_);
      cond_.notify_all();
    }
  }

 private:
  std::mutex mutex_;                       // 保护条件变量的互斥锁
  std::condition_variable cond_;           // 条件变量，用于线程等待和通知
  std::atomic<int> reader_count_;          // 当前活跃的读者数量，使用原子变量
  std::atomic<bool> writer_active_;        // 是否有写者活跃，使用原子变量
  std::atomic<int> writer_waiting_;        // 等待获取写锁的写者数量，使用原子变量
};

}  // namespace bustub
