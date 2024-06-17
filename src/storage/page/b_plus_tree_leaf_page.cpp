//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetMaxSize(max_size);
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(-1);
  SetPrvPageId(-1);
  SetFather(-1);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetPrvPageId() const -> page_id_t { return prv_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetPrvPageId(bustub::page_id_t prv_page_id) { prv_page_id_ = prv_page_id; }
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Setpoint(KeyType &key, ValueType &value, int index) {
  array_[index].first = key;
  array_[index].second = value;
  IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetFather(bustub::page_id_t page) { father_ = page; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetFather() const -> page_id_t { return father_; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SearchKkey(const KeyType &value, KeyComparator &cmp) -> int {
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
  return r;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Searchkey(const KeyType &value, KeyComparator &cmp,
                                           std::vector<ValueType> *result) const -> bool {
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
  if (cmp(value, array_[r].first) == 0) {
    result->push_back(array_[r].second);
    return true;
  }
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Setarray(const KeyType &key, const ValueType &value) {
  array_[GetSize()].first = key;
  array_[GetSize()].second = value;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, KeyComparator &cmp) {
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
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Spilt(BPlusTreeLeafPage *leaf) -> KeyType {
  int mid = (this->GetMaxSize()) / 2;
  leaf->SetMaxSize(this->GetMaxSize());
  leaf->SetSize(this->GetSize() - mid);
  for (int i = mid; i < GetSize(); i++) {
    leaf->array_[i - mid] = this->array_[i];
  }
  this->SetSize(mid);
  return leaf->array_[0].first;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Delete(const KeyType &key, KeyComparator &cmp) {
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
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
