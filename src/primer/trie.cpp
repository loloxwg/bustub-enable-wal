
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <queue>
#include <string_view>
#include <unordered_map>

#include "common/exception.h"
#include "common/macros.h"
#include "primer/trie.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (!this->root_) {
    return nullptr;
  }

  auto cur_node = this->root_;
  for (auto ch : key) {
    auto node = cur_node->children_.find(ch);
    if (node == cur_node->children_.end()) {
      return nullptr;
    }
    cur_node = node->second;
  }

  if (cur_node->is_value_node_) {
    auto node = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(cur_node);
    if (node) {
      return node->value_.get();
    }
  }
  return nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

auto Trie::Find(std::string_view key) const -> bool {
  if (!this->root_) {
    return false;
  }

  auto cur_node = this->root_;
  for (auto ch : key) {
    auto node = cur_node->children_.find(ch);
    if (node == cur_node->children_.end()) {
      return false;
    }
    cur_node = node->second;
  }
  return cur_node->is_value_node_;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  if (key.empty()) {
    auto node = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    if (this->root_) {
      for (const auto &iter : this->root_->children_) {
        node->children_[iter.first] = iter.second;
      }
    }
    return Trie{node};
  }

  std::shared_ptr<TrieNode> new_root;
  if (this->root_) {
    new_root = this->root_->Clone();
  } else {
    new_root = std::make_shared<TrieNode>();
  }
  auto pos = key.size();
  auto cur_node = new_root;

  for (auto ch : key) {
    auto node = cur_node->children_.find(ch);
    pos--;

    if (node == cur_node->children_.end()) {
      if (pos == 0) {
        cur_node->children_[ch] = std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      } else {
        cur_node->children_[ch] = std::make_shared<const TrieNode>();
      }
      cur_node = std::const_pointer_cast<TrieNode>(cur_node->children_[ch]);
    } else if (pos == 0) {
      auto new_node =
          std::make_shared<TrieNodeWithValue<T>>(node->second->children_, std::make_shared<T>(std::move(value)));
      cur_node->children_[ch] = new_node;
      cur_node = new_node;
    } else {
      auto new_node = std::shared_ptr<TrieNode>(node->second->Clone());
      cur_node->children_[ch] = new_node;
      cur_node = new_node;
    }
  }
  return Trie{new_root};
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::RemoveRecursive(std::string_view key, const std::shared_ptr<const TrieNode> &root) const
    -> std::shared_ptr<TrieNode> {
  auto new_root = root->Clone();
  auto node = new_root->children_[key.at(0)];

  if (key.size() > 1) {
    auto new_node = this->RemoveRecursive(key.substr(1), node);
    if (new_node && (new_node->is_value_node_ || !new_node->children_.empty())) {
      new_root->children_[key.at(0)] = new_node;
    } else {
      new_root->children_.erase(key.at(0));
    }
    if (new_root->is_value_node_ || !new_root->children_.empty()) {
      return new_root;
    }
    return nullptr;
  }

  BUSTUB_ASSERT(node->is_value_node_, "TrieNode isn't value node!");

  if (node->children_.empty()) {
    new_root->children_.erase(key.at(0));
    if (new_root->is_value_node_ || !new_root->children_.empty()) {
      return new_root;
    }
    return nullptr;
  }
  auto new_child = std::make_shared<TrieNode>(node->children_);
  new_root->children_[key.at(0)] = new_child;
  return new_root;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (!this->Find(key)) {
    return Trie{root_};
  }
  if (key.empty()) {
    if (this->root_->is_value_node_) {
      if (this->root_->children_.empty()) {
        return Trie{};
      }
      return Trie{std::make_shared<TrieNode>(this->root_->children_)};
    }
  }

  if (!this->root_) {
    return Trie{};
  }

  auto new_root = this->RemoveRecursive(key, std::const_pointer_cast<TrieNode>(this->root_));
  if (new_root && !new_root->children_.empty()) {
    return Trie{new_root};
  }
  return Trie{};
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
