//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// tuple_test.cpp
//
// Identification: test/table/tuple_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "logging/common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"

namespace bustub {
// NOLINTNEXTLINE
TEST(TupleTest, TableHeapTest) {
  // test1: parse create sql statement
  std::string create_stmt = "a varchar(20), b smallint, c bigint, d bool, e varchar(16)";
  Column col1{"a", TypeId::VARCHAR, 20};
  Column col2{"b", TypeId::SMALLINT};
  Column col3{"c", TypeId::BIGINT};
  Column col4{"d", TypeId::BOOLEAN};
  Column col5{"e", TypeId::VARCHAR, 16};
  std::vector<Column> cols{col1, col2, col3, col4, col5};
  Schema schema{cols};
  Tuple tuple = ConstructTuple(&schema);

  // create transaction
  auto *disk_manager = new DiskManager("test.db");
  auto *buffer_pool_manager = new BufferPoolManager(50, disk_manager);
  auto *table = new TableHeap(buffer_pool_manager);

  std::vector<RID> rid_v;
  for (int i = 0; i < 100000; ++i) {
    auto rid = table->InsertTuple(TupleMeta{0, false}, tuple);
    // Verify that insertion was successful
    rid_v.push_back(*rid);
  }

  // Verify the number of inserted tuples
  EXPECT_EQ(rid_v.size(), 100000);

  // Verify that we can retrieve all inserted tuples
  int tuple_count = 0;
  TableIterator itr = table->MakeIterator();
  while (!itr.IsEnd()) {
    // Verify tuple data matches what we inserted
    auto rid = itr.GetRID();
    EXPECT_EQ(rid.GetSlotNum(), rid_v[tuple_count].GetSlotNum());
    EXPECT_EQ(rid.GetPageId(), rid_v[tuple_count].GetPageId());
    auto [tuple_meta, fetched_tuple] = itr.GetTuple();
    // 比较元组内容是否相同
    EXPECT_TRUE(IsTupleContentEqual(fetched_tuple, tuple));
    ++itr;
    tuple_count++;
  }

  // Verify that we can iterate through all inserted tuples
  EXPECT_EQ(tuple_count, 100000);

  disk_manager->ShutDown();
  remove("test.db");  // remove db file
  remove("test.log");
  delete table;
  delete buffer_pool_manager;
  delete disk_manager;
}

}  // namespace bustub
