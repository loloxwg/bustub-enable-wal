//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"
#include "common/macros.h"
#include "fmt/core.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) { max_depth_ = max_depth; }

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  return hash >> (32 - max_depth_ - 1);
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  BUSTUB_ENSURE(directory_idx >= 0 && directory_idx < MaxSize(),
                fmt::format("directory_idx {} should between {} and {}", directory_idx, 0, MaxSize()));
  return directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  BUSTUB_ENSURE(directory_idx >= 0 && directory_idx < MaxSize(),
                fmt::format("directory_idx {} should between {} and {}", directory_idx, 0, MaxSize()));
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return 1 << max_depth_; }

}  // namespace bustub
