//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetPrvPageId(-1);
  SetNextPageId(-1);
  SetFather(-1);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetPrvPageId() const -> page_id_t { return prv_page_id_; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetFather(bustub::page_id_t page) { father_ = page; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetFather() const -> page_id_t { return father_; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetPrvPageId(bustub::page_id_t prv_page_id) { prv_page_id_ = prv_page_id; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Setpoint(KeyType &key, ValueType &value, int index) {
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(const KeyType &key, KeyComparator &cmp) {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (cmp(key, array_[mid].first) >= 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  if (cmp(key, array_[r].first) == 0) {
    for (int i = r; i < GetSize(); i++) {
      array_[i] = array_[i + 1];
    }
    IncreaseSize(-1);
  }
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const page_id_t &value, KeyComparator &cmp) {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (cmp(key, array_[mid].first) > 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  r++;
  for (int i = GetSize(); i > r; i--) {
    array_[i] = array_[i - 1];
  }
  array_[r].first = key;
  array_[r].second = value;
  IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const page_id_t &value) { array_[index].second = value; }
/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i <= GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return 0;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Searchkey(const KeyType &value, KeyComparator &cmp) const -> int {
  int l = 0;
  int r = GetSize() - 1;
  while (l <= r) {
    int mid = (l + r) >> 1;
    if (cmp(value, array_[mid].first) >= 0) {
      l = mid + 1;
    } else {
      r = mid - 1;
    }
  }
  if (r == -1) {
    r = 0;
  }
  return array_[r].second;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Spilt(BPlusTreeInternalPage *leaf) -> KeyType {
  int mid = (this->GetSize()) / 2;
  leaf->SetMaxSize(this->GetMaxSize());
  leaf->SetSize(this->GetSize() - mid);
  for (int i = mid; i < GetSize(); i++) {
    leaf->array_[i - mid] = this->array_[i];
  }
  this->SetSize(mid);
  return leaf->array_[0].first;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
