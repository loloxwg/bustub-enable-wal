//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_test.cpp
//
// Identification: test/container/disk/hash/extendible_htable_test.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <_types/_uint32_t.h>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/column.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/generic_key.h"
#include "type/type_id.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 0, 2, 2);

  int num_keys = 8;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // attempt another insert, this should fail because table is full
  ASSERT_FALSE(ht.Insert(num_keys, num_keys));
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, InsertTest3) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  // insert some values
  //  000 0
  //  100 4
  // 1000 8
  // 1100 12
  //  010  2
  //  110  6
  //  001  1
  //  011  3
  //  101  5
  int keys[] = {0, 4, 8, 12, 2, 6, 1, 3, 5, 11, 9};
  int miss[] = {7};
  for (auto key : keys) {
    bool inserted = ht.Insert(key, key);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(key, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(key, res[0]);
    ht.VerifyIntegrity();
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (auto key : keys) {
    std::vector<int> res;
    bool got_value = ht.GetValue(key, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(key, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (auto key : miss) {
    std::vector<int> res;
    bool got_value = ht.GetValue(key, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

// NOLINTNEXTLINE
TEST(ExtendibleHTableTest, RemoveTest1) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  int num_keys = 5;

  // insert some values
  for (int i = 0; i < num_keys; i++) {
    bool inserted = ht.Insert(i, i);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (int i = 0; i < num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(i, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
  // remove the keys we inserted
  for (int i = 0; i < num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_TRUE(removed);
    std::vector<int> res;
    ht.GetValue(i, &res);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();

  // try to remove some keys that don't exist/were not inserted
  for (int i = num_keys; i < 2 * num_keys; i++) {
    bool removed = ht.Remove(i);
    ASSERT_FALSE(removed);
    std::vector<int> res;
    bool got_value = ht.GetValue(i, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, RemoveTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  // insert some values
  //  000 0
  //  100 4
  // 1000 8
  // 1100 12
  //  010  2
  //  110  6
  //  001  1
  //  011  3
  //  101  5
  int keys[] = {0, 4, 8, 12, 2, 6, 1, 3, 5, 9, 11};
  int miss[] = {7};
  for (auto key : keys) {
    bool inserted = ht.Insert(key, key);
    ASSERT_TRUE(inserted);
    std::vector<int> res;
    ht.GetValue(key, &res);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(key, res[0]);
    ht.VerifyIntegrity();
  }

  ht.VerifyIntegrity();

  // check that they were actually inserted
  for (auto key : keys) {
    std::vector<int> res;
    bool got_value = ht.GetValue(key, &res);
    ASSERT_TRUE(got_value);
    ASSERT_EQ(1, res.size());
    ASSERT_EQ(key, res[0]);
  }

  ht.VerifyIntegrity();

  // try to get some keys that don't exist/were not inserted
  for (auto key : miss) {
    std::vector<int> res;
    bool got_value = ht.GetValue(key, &res);
    ASSERT_FALSE(got_value);
    ASSERT_EQ(0, res.size());
  }

  // try to remove some keys to merge
  for (auto key : keys) {
    ASSERT_TRUE(ht.Remove(key));
    std::cout << "After Remove ====== " << key << " ======" << std::endl;
    ht.VerifyIntegrity();
  }
}

TEST(ExtendibleHTableTest, RemoveTest3) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(50, disk_mgr.get());

  DiskExtendibleHashTable<int, int, IntComparator> ht("blah", bpm.get(), IntComparator(), HashFunction<int>(), 2, 3, 2);

  ASSERT_TRUE(ht.Insert(4, 4, nullptr));
  std::vector<int> res;
  bool got_value = ht.GetValue(4, &res);
  ASSERT_TRUE(got_value);
  ASSERT_EQ(1, res.size());
  ASSERT_EQ(4, res[0]);
  ht.VerifyIntegrity();

  res.clear();
  ASSERT_TRUE(ht.Insert(5, 5, nullptr));
  got_value = ht.GetValue(5, &res);
  ASSERT_TRUE(got_value);
  ASSERT_EQ(1, res.size());
  ASSERT_EQ(5, res[0]);
  ht.VerifyIntegrity();

  res.clear();
  ASSERT_TRUE(ht.Insert(6, 6, nullptr));
  got_value = ht.GetValue(6, &res);
  ASSERT_TRUE(got_value);
  ASSERT_EQ(1, res.size());
  ASSERT_EQ(6, res[0]);
  ht.VerifyIntegrity();

  res.clear();
  ASSERT_TRUE(ht.Insert(14, 14, nullptr));
  got_value = ht.GetValue(14, &res);
  ASSERT_TRUE(got_value);
  ASSERT_EQ(1, res.size());
  ASSERT_EQ(14, res[0]);
  ht.VerifyIntegrity();

  res.clear();
  ASSERT_TRUE(ht.Remove(5, nullptr));
  got_value = ht.GetValue(5, &res);
  ASSERT_FALSE(got_value);
  ASSERT_EQ(0, res.size());
  ht.VerifyIntegrity();

  res.clear();
  ASSERT_TRUE(ht.Remove(14, nullptr));
  got_value = ht.GetValue(14, &res);
  ASSERT_FALSE(got_value);
  ASSERT_EQ(0, res.size());
  ht.VerifyIntegrity();
  // 14 8 + 6
  // 4  0100
  // 5  0101
  // 6  0110
  // 14 1110
  res.clear();
  ASSERT_TRUE(ht.Remove(4, nullptr));
  got_value = ht.GetValue(4, &res);
  ASSERT_FALSE(got_value);
  ASSERT_EQ(0, res.size());
  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, DebugTest9) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_mgr.get());
  DiskExtendibleHashTable<int, int, IntComparator> ht("debug_test", bpm.get(), IntComparator(), HashFunction<int>(), 9,
                                                      9, 511);
  std::set<int> keys;

  for (int i = 0; i <= 999; i++) {
    ASSERT_TRUE(ht.Insert(i, i));
    // ASSERT_TRUE(ht.Remove(10));
  }
  for (int i = 0; i <= 499; i++) {
    ASSERT_TRUE(ht.Remove(i));
    // ASSERT_TRUE(ht.Remove(10));
  }
  for (int i = 1000; i <= 1499; i++) {
    ASSERT_TRUE(ht.Insert(i, i));
    // ASSERT_TRUE(ht.Remove(10));
  }
  for (int i = 500; i <= 999; i++) {
    ASSERT_TRUE(ht.Remove(i));
    // ASSERT_TRUE(ht.Remove(10));
  }
  for (int i = 0; i <= 499; i++) {
    ASSERT_TRUE(ht.Insert(i, i));
    // ASSERT_TRUE(ht.Remove(10));
  }
  for (int i = 1000; i <= 1499; i++) {
    ASSERT_TRUE(ht.Remove(i));
    // ASSERT_TRUE(ht.Remove(10));
  }
  for (int i = 0; i <= 499; i++) {
    ASSERT_TRUE(ht.Remove(i));
    // ASSERT_TRUE(ht.Remove(10));
  }
  ht.PrintHT();
  ht.VerifyIntegrity();
}

TEST(ExtendibleHTableTest, DebugTest) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(34, disk_mgr.get());
  DiskExtendibleHashTable<int, int, IntComparator> ht("debug_test", bpm.get(), IntComparator(), HashFunction<int>(), 9,
                                                      9, 2);
  std::set<int> keys;

  size_t max_num = 64;

  for (int i = 1; i < 10000; i++) {
    auto r = rand() % 1000;
    if ((r % 3 == 0 || r % 7 == 0 || r % 11 == 0) && !keys.empty() && keys.size() >= max_num - 5) {
      std::vector<int> vec(keys.begin(), keys.end());
      auto s = rand() % vec.size();
      ASSERT_TRUE(ht.Remove(vec[s]));
      auto it = keys.find(vec[s]);
      keys.erase(it);
    } else {
      if (ht.Insert(r, r)) {
        keys.insert(r);
      } else if (keys.find(r) == keys.end()) {
        if (keys.size() < max_num) {
          std::cout << keys.size() << "<>" << max_num << " " << r << std::endl;
          ht.PrintHT();
          std::terminate();
        }
      }
    }
    ht.VerifyIntegrity();
  }

  for (auto it : keys) {
    ASSERT_TRUE(ht.Remove(it));
    ht.VerifyIntegrity();
  }
}

TEST(ExtendibleHTableTest, DebugTest2) {
  auto disk_mgr = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(5, disk_mgr.get());
  using IntegerComparatorType = GenericComparator<8>;
  Column c("c1", TypeId::INTEGER);
  auto sh = Schema({c});
  const auto cmp = IntegerComparatorType{&sh};
  DiskExtendibleHashTable<GenericKey<8>, RID, IntegerComparatorType> ht("debug_test", bpm.get(), cmp,
                                                                        HashFunction<GenericKey<8>>(), 9, 9, 255);
  for (int i = 0; i <= 256; i++) {
    GenericKey<8> key;
    key.SetFromInteger(i);
    RID rid{0, static_cast<uint32_t>(i)};
    ASSERT_TRUE(ht.Insert(key, rid));
    ht.VerifyIntegrity();
  }
  for (int i = 257; i <= 499; i++) {
    GenericKey<8> key;
    key.SetFromInteger(i);
    RID rid{0, static_cast<uint32_t>(i)};
    ASSERT_TRUE(ht.Insert(key, rid));
    ht.VerifyIntegrity();
  }
  ht.VerifyIntegrity();
}

}  // namespace bustub
