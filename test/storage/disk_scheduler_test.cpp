//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_manager_test.cpp
//
// Identification: test/storage/disk/disk_scheduler_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstring>
#include <future>  // NOLINT
#include <memory>
#include <thread>

#include "common/exception.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/disk/disk_scheduler.h"

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// NOLINTNEXTLINE
TEST(DiskSchedulerTest, ScheduleWriteReadPageTest) {
  char buf[BUSTUB_PAGE_SIZE] = {0};
  char data[BUSTUB_PAGE_SIZE] = {0};

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_shared<DiskScheduler>(dm.get());

  std::strncpy(data, "A test string.", sizeof(data));

  auto promise1 = disk_scheduler->CreatePromise();
  auto future1 = promise1.get_future();
  auto promise2 = disk_scheduler->CreatePromise();
  auto future2 = promise2.get_future();

  disk_scheduler->Schedule({/*is_write=*/true, reinterpret_cast<char *>(&data), /*page_id=*/0, std::move(promise1)});
  disk_scheduler->Schedule({/*is_write=*/false, reinterpret_cast<char *>(&buf), /*page_id=*/0, std::move(promise2)});

  ASSERT_TRUE(future1.get());
  ASSERT_TRUE(future2.get());
  ASSERT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  dm->ShutDown();
}

TEST(DiskSchedulerTest, ScheduleWriteReadPageTest2) {
  char buf[BUSTUB_PAGE_SIZE] = {0};
  char data[BUSTUB_PAGE_SIZE] = {0};

  auto dm = std::make_unique<DiskManagerUnlimitedMemory>();
  auto disk_scheduler = std::make_unique<DiskScheduler>(dm.get());

  std::strncpy(data, "A test string.", sizeof(data));

  auto promise1 = disk_scheduler->CreatePromise();
  auto future1 = promise1.get_future();
  auto promise2 = disk_scheduler->CreatePromise();
  auto future2 = promise2.get_future();

  std::thread t1([&disk_scheduler]() {
    std::cout << "begin" << std::endl;
    auto promise = disk_scheduler->CreatePromise();
    auto future = promise.get_future();
    auto promise1 = disk_scheduler->CreatePromise();
    auto future1 = promise1.get_future();
    char data[BUSTUB_PAGE_SIZE] = {0};
    char data2[BUSTUB_PAGE_SIZE] = {0};
    std::strncpy(data, "NONONONO", sizeof(data));
    disk_scheduler->Schedule({/*is_write=*/true, reinterpret_cast<char *>(&data), /*page_id=*/0, std::move(promise)});
    ASSERT_TRUE(future.get());
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    disk_scheduler->Schedule(
        {/*is_write=*/false, reinterpret_cast<char *>(&data2), /*page_id=*/0, std::move(promise1)});
    ASSERT_TRUE(future1.get());
    std::cout << "result: " << std::string(data2, 20) << std::endl;
    std::cout << "end" << std::endl;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  disk_scheduler->Schedule({/*is_write=*/true, reinterpret_cast<char *>(&data), /*page_id=*/0, std::move(promise1)});
  disk_scheduler->Schedule({/*is_write=*/false, reinterpret_cast<char *>(&buf), /*page_id=*/0, std::move(promise2)});

  ASSERT_TRUE(future1.get());
  ASSERT_TRUE(future2.get());
  ASSERT_EQ(std::memcmp(buf, data, sizeof(buf)), 0);

  t1.join();
  disk_scheduler = nullptr;  // Call the DiskScheduler destructor to finish all scheduled jobs.
  dm->ShutDown();
  std::cout << "now" << std::endl;
}

}  // namespace bustub
