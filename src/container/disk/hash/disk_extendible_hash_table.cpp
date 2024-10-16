//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "concurrency/transaction.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  BasicPageGuard header_page_gurad = bpm_->NewPageGuarded(&header_page_id_);
  auto header_page = header_page_gurad.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  ReadPageGuard header_page_gurad = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_gurad.As<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard directory_page_gurad = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_page_gurad.As<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  ReadPageGuard bucket_page_gurad = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_page_gurad.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  if (bucket_page->Lookup(key, value, cmp_)) {
    result->emplace_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  BasicPageGuard header_page_gurad = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_page_gurad.As<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    WritePageGuard header_write_page_gurad = header_page_gurad.UpgradeWrite();
    auto header_page = header_write_page_gurad.AsMut<ExtendibleHTableHeaderPage>();
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }

  WritePageGuard directory_write_page_gurad = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_write_page_gurad.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(directory_page, bucket_index, key, value);
  }
  WritePageGuard bucket_page_gurad = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_page_gurad.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (!bucket_page->IsFull()) {
    return bucket_page->Insert(key, value, cmp_);
  }

  auto local_depth = directory_page->GetLocalDepth(bucket_index);
  if (local_depth == directory_max_depth_) {
    return false;
  }

  // directory_page->SetLocalDepth(bucket_index, local_depth + 1);
  if (local_depth == directory_page->GetGlobalDepth()) {
    if (directory_page->GetMaxDepth() == directory_page->GetGlobalDepth()) {
      return false;
    }
    directory_page->IncrGlobalDepth();
  }

  std::vector<uint32_t> bucket_vec1;
  std::vector<uint32_t> bucket_vec2;

  for (uint32_t i = 0; i < directory_page->Size(); i++) {
    if (directory_page->GetBucketPageId(i) == bucket_page_id) {
      directory_page->IncrLocalDepth(i);
      if ((i & (1 << local_depth)) == 0) {
        bucket_vec1.push_back(i);
      } else {
        bucket_vec2.push_back(i);
      }
    }
  }

  uint32_t bucket_num = 1 << (directory_page->GetGlobalDepth() - local_depth);
  assert(bucket_vec1.size() == bucket_vec2.size());
  assert(bucket_vec1.size() + bucket_vec2.size() == bucket_num);
  // 2^(GD - LD)

  // auto split_index = directory_page->GetSplitImageIndex(bucket_index);
  // directory_page->SetLocalDepth(split_index, local_depth + 1);

  page_id_t new_page_id;
  BasicPageGuard new_bucket_page_guard = bpm_->NewPageGuarded(&new_page_id);
  if (new_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard new_bucket_write_page_guard = new_bucket_page_guard.UpgradeWrite();

  if ((bucket_index & (1 << local_depth)) == 0) {
    for (const auto &i : bucket_vec2) {
      directory_page->SetBucketPageId(i, new_page_id);
    }
  } else {
    for (const auto &i : bucket_vec1) {
      directory_page->SetBucketPageId(i, new_page_id);
    }
  }

  directory_page->PrintDirectory();
  auto new_bucket_page = new_bucket_write_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);
  std::vector<std::pair<K, V>> elements;
  for (uint32_t i = 0; i < bucket_page->Size(); i++) {
    elements.emplace_back(bucket_page->EntryAt(i));
  }
  bucket_page->Clear();
  for (const auto &pair : elements) {
    auto entry_hash = Hash(pair.first);
    auto entry_bucket_index = directory_page->HashToBucketIndex(entry_hash);
    auto entry_page_id = directory_page->GetBucketPageId(entry_bucket_index);
    if (entry_page_id == bucket_page_id) {
      assert(bucket_page->Insert(pair.first, pair.second, cmp_));
    } else if (entry_page_id == new_page_id) {
      assert(new_bucket_page->Insert(pair.first, pair.second, cmp_));
    } else {
      assert(false);
    }
  }
  header_page_gurad.Drop();
  directory_write_page_gurad.Drop();
  bucket_page_gurad.Drop();
  new_bucket_write_page_guard.Drop();
  return Insert(key, value, transaction);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t page_id;
  BasicPageGuard directory_page_gurad = bpm_->NewPageGuarded(&page_id);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  WritePageGuard directory_write_page_gurad = directory_page_gurad.UpgradeWrite();
  auto directory_page = directory_write_page_gurad.AsMut<ExtendibleHTableDirectoryPage>();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, page_id);
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  assert(bucket_page_id == INVALID_PAGE_ID);
  return InsertToNewBucket(directory_page, bucket_index, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  BasicPageGuard bucket_page_gurad = bpm_->NewPageGuarded(&bucket_page_id);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  WritePageGuard bucket_write_page_gurad = bucket_page_gurad.UpgradeWrite();
  auto bucket_page = bucket_write_page_gurad.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  directory->SetLocalDepth(new_bucket_idx, new_local_depth);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  BasicPageGuard header_page_gurad = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_page_gurad.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false;
  }
  BasicPageGuard directory_page_gurad = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_page_gurad.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }
  BasicPageGuard bucket_page_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_page->IsEmpty()) {
    return false;
  }
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }
  if (!bucket_page->IsEmpty()) {
    return true;
  }

  if (directory_page->GetGlobalDepth() == 0) {
    assert(bucket_index == 0);
    directory_page->SetBucketPageId(bucket_index, INVALID_PAGE_ID);
    return true;
  }

  std::vector<uint32_t> bucket_vec;
  for (uint32_t i = 0; i < directory_page->Size(); i++) {
    if (directory_page->GetBucketPageId(i) == bucket_page_id) {
      bucket_vec.emplace_back(i);
      directory_page->SetBucketPageId(i, INVALID_PAGE_ID);
    }
  }

  bpm_->DeletePage(bucket_page_id);
  Merge(directory_page, bucket_vec);
  return true;
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::Merge(ExtendibleHTableDirectoryPage *directory_page,
                                              const std::vector<uint32_t> &bucket_idx_vec) {
  assert(!bucket_idx_vec.empty());

  if (directory_page->GetGlobalDepth() == 0) {
    assert(bucket_idx_vec.size() == 1 && bucket_idx_vec[0] == 0);
    directory_page->SetBucketPageId(0, INVALID_PAGE_ID);
    return;
  }

  auto local_depth = directory_page->GetLocalDepth(bucket_idx_vec[0]);
  std::vector<uint32_t> split_idx_vec;
  split_idx_vec.reserve(bucket_idx_vec.size());
  uint32_t bucket_num = 1 << (directory_page->GetGlobalDepth() - local_depth);

  for (uint32_t i : bucket_idx_vec) {
    uint32_t split_image_index = directory_page->GetSplitImageIndex(i);
    split_idx_vec.emplace_back(split_image_index);
    if (directory_page->GetBucketPageId(split_image_index) != INVALID_PAGE_ID) {
      return;
    }
  }
  assert(bucket_idx_vec.size() == split_idx_vec.size() && bucket_idx_vec.size() == bucket_num);

  // merge
  std::vector<uint32_t> merge_bucket_idx;
  merge_bucket_idx.reserve(bucket_idx_vec.size() + split_idx_vec.size());
  for (const auto idx : bucket_idx_vec) {
    directory_page->DecrLocalDepth(idx);
  }
  for (const auto idx : split_idx_vec) {
    directory_page->DecrLocalDepth(idx);
  }

  if (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }
  for (const auto idx : bucket_idx_vec) {
    if (idx < directory_page->Size()) {
      merge_bucket_idx.emplace_back(idx);
    }
  }
  for (const auto idx : split_idx_vec) {
    if (idx < directory_page->Size()) {
      merge_bucket_idx.emplace_back(idx);
    }
  }

  // merge recursively
  Merge(directory_page, merge_bucket_idx);
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
