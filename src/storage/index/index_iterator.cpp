/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, page_id_t page, int tmp) {
  bpm_ = bpm;
  page_ = page;
  size_ = tmp;
  if (page_ != -1) {
    auto leaf = bpm_->FetchPageRead(page_);
    auto leaf_page = leaf.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    item_ = MappingType(leaf_page->KeyAt(size_), leaf_page->ValueAt(size_));
  }
}
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_ == -1; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return this->item_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  auto leaf = bpm_->FetchPageRead(page_);
  auto leaf_page = leaf.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  size_++;
  if (size_ == leaf_page->GetSize()) {
    auto next_id = leaf_page->GetNextPageId();
    if (next_id != -1) {
      size_ = 0;
      page_ = next_id;
      auto guard = bpm_->FetchPageRead(page_);
      auto leaf1 = guard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
      item_ = {leaf1->KeyAt(size_), leaf1->ValueAt(size_)};
    } else {
      page_ = -1;
      size_ = -1;
      item_ = {};
    }
  } else {
    item_ = {leaf_page->KeyAt(size_), leaf_page->ValueAt(size_)};
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
