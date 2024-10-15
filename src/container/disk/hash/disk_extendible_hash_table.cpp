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
  BasicPageGuard header_page_gurad = bpm_->FetchPageBasic(header_page_id_);
  auto header_page = header_page_gurad.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == 0) {
    return false;
  }
  BasicPageGuard directory_page_gurad = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_page_gurad.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == 0) {
    return false;
  }
  BasicPageGuard bucket_page_gurad = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_page_gurad.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
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
  auto header_page = header_page_gurad.AsMut<ExtendibleHTableHeaderPage>();
  auto hash = Hash(key);
  auto directory_idx = header_page->HashToDirectoryIndex(hash);
  auto directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == 0) {
    page_id_t page_id;
    BasicPageGuard directory_page_gurad = bpm_->NewPageGuarded(&page_id);
    auto directory_page = directory_page_gurad.AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
    header_page->SetDirectoryPageId(directory_idx, page_id);
  }
  return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  ;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  auto directory_page_id = header->GetDirectoryPageId(directory_idx);
  BasicPageGuard directory_page_gurad = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_page_gurad.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(Hash(key));
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);

  if (bucket_page_id == 0) {
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

  do {
    directory_page->SetLocalDepth(bucket_index, local_depth + 1);
    if (local_depth == directory_page->GetGlobalDepth()) {
      directory_page->IncrGlobalDepth();
    }
    auto split_index = directory_page->GetSplitImageIndex(bucket_index);
    directory_page->SetLocalDepth(split_index, local_depth + 1);

    page_id_t new_page_id_1;
    page_id_t new_page_id_2;
    BasicPageGuard new_bucket_page_guard_1 = bpm_->NewPageGuarded(&new_page_id_1);
    BasicPageGuard new_bucket_page_guard_2 = bpm_->NewPageGuarded(&new_page_id_2);
    directory_page->SetBucketPageId(bucket_index, new_page_id_1);
    directory_page->SetBucketPageId(split_index, new_page_id_2);
    auto new_bucket_page_1 = new_bucket_page_guard_1.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    auto new_bucket_page_2 = new_bucket_page_guard_2.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page_1->Init(bucket_max_size_);
    new_bucket_page_2->Init(bucket_max_size_);

    for (uint32_t i = 0; i < bucket_page->Size(); i++) {
      auto entry = bucket_page->EntryAt(i);
      auto entry_hash = Hash(entry.first);
      auto new_bucket_idx = directory_page->HashToBucketIndex(entry_hash);
      if (new_bucket_idx != bucket_index) {
        if (!new_bucket_page_2->Insert(entry.first, entry.second, cmp_)) {
          throw Exception("Inserted Wrong");
        }
      } else {
        if (!new_bucket_page_1->Insert(entry.first, entry.second, cmp_)) {
          throw Exception("Inserted Wrong");
        }
      }
    }

    auto new_bucket_index = directory_page->HashToBucketIndex(Hash(key));
    if (new_bucket_index == bucket_index) {
      if (new_bucket_page_1->IsFull()) {
        bucket_page_id = new_page_id_1;
      } else {
        return new_bucket_page_1->Insert(key, value, cmp_);
      }
    } else {
      if (new_bucket_page_2->IsFull()) {
        bucket_page_id = new_page_id_2;
      } else {
        return new_bucket_page_2->Insert(key, value, cmp_);
      }
    }

    bucket_page_gurad = bpm_->FetchPageWrite(bucket_page_id);
    bucket_page = bucket_page_gurad.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bpm_->DeletePage(bucket_page_id);

  } while (true);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  BasicPageGuard bucket_page_gurad = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_page = bucket_page_gurad.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
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
  if (directory_page_id == 0) {
    return false;
  }
  BasicPageGuard directory_page_gurad = bpm_->FetchPageBasic(directory_page_id);
  auto directory_page = directory_page_gurad.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = directory_page->HashToBucketIndex(hash);
  auto bucket_page_id = directory_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == 0) {
    return false;
  }
  BasicPageGuard bucket_page_guard = bpm_->FetchPageBasic(bucket_page_id);
  auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_page->IsEmpty()) {
    return false;
  }
  V value;
  if (!bucket_page->Lookup(key, value, cmp_)) {
    return false;
  }
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }
  return true;

  do {
    if (bucket_page->IsEmpty()) {
      auto local_depth = directory_page->GetLocalDepth(bucket_index);
      if (local_depth == 0) {
        directory_page->SetBucketPageId(bucket_index, 0);
        directory_page_gurad.Drop();
        bpm_->DeletePage(directory_page_id);
        return true;
      }

      auto split_bucket_index = directory_page->GetSplitImageIndex(bucket_index);
      auto split_bucket_page_id = directory_page->GetBucketPageId(split_bucket_index);
      if (split_bucket_page_id != bucket_page_id) {
        BasicPageGuard split_bucket_page_guard = bpm_->FetchPageBasic(split_bucket_page_id);
        auto split_bucket_page = split_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        if (!split_bucket_page->IsEmpty()) {
          return true;
        }

        directory_page->SetBucketPageId(bucket_index, bucket_page_id);
        directory_page->SetBucketPageId(split_bucket_index, bucket_page_id);
        split_bucket_page_guard.Drop();
        bpm_->DeletePage(split_bucket_page_id);
      }
      directory_page->SetLocalDepth(bucket_index, local_depth - 1);
      directory_page->SetLocalDepth(split_bucket_index, local_depth - 1);

      if (split_bucket_index < bucket_index) {
        bucket_index = split_bucket_index;
      }
      bucket_page_id = directory_page->GetBucketPageId(bucket_index);
      bucket_page_guard = bpm_->FetchPageBasic(bucket_page_id);
      bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    } else {
      return true;
    }
  } while (true);
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
