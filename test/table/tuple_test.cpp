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
#include <chrono>

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


  // create transaction
  auto *disk_manager = new DiskManager("test.db");
  auto *buffer_pool_manager = new BufferPoolManager(5, disk_manager);
  auto *table = new TableHeap(buffer_pool_manager);

  int nums = 1000000;
  std::vector<Tuple> tuple_v;
  std::vector<RID> rid_v;

  // 开始插入计时
  auto insert_start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < nums ; ++i) {
    Tuple tuple = ConstructTuple(&schema);
    auto rid = table->InsertTuple(TupleMeta{0, false}, tuple);
    rid_v.push_back(*rid);
    tuple_v.push_back(tuple);
  }

  // 结束插入计时并输出结果
  auto insert_end = std::chrono::high_resolution_clock::now();
  auto insert_duration = std::chrono::duration_cast<std::chrono::microseconds>(insert_end - insert_start);
  std::cout << "Insertion of " << nums << " tuples took " << insert_duration.count() << " μs" << std::endl;
  std::cout << "Average insertion time per tuple: " << static_cast<double>(insert_duration.count()) / nums << " μs" << std::endl;

  // 开始查询计时
  auto query_start = std::chrono::high_resolution_clock::now();

  TableIterator itr = table->MakeIterator();
  int cnt = 0;
  while (!itr.IsEnd()) {
    // std::cout << itr->ToString(schema) << std::endl;
    //EXPECT_TRUE(IsTupleContentEqual(itr.GetTuple().second,tuple_v[cnt]));

    ++itr;
    ++cnt;
  }

  // 结束查询计时并输出结果
  auto query_end = std::chrono::high_resolution_clock::now();
  auto query_duration = std::chrono::duration_cast<std::chrono::microseconds>(query_end - query_start);
  std::cout << "Query of " << cnt << " tuples took " << query_duration.count() << " μs" << std::endl;
  std::cout << "Average query time per tuple: " << static_cast<double>(query_duration.count()) / cnt << " μs" << std::endl;

  disk_manager->ShutDown();
  remove("test.db");  // remove db file
  remove("test.log");
  delete table;
  delete buffer_pool_manager;
  delete disk_manager;
}

}  // namespace bustub
