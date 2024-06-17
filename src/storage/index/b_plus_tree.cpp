#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  if (leaf_max_size_ > static_cast<int>(LEAF_PAGE_SIZE)) {
    leaf_max_size_ = LEAF_PAGE_SIZE;
  }
  if (internal_max_size_ > static_cast<int>(INTERNAL_PAGE_SIZE)) {
    internal_max_size_ = INTERNAL_PAGE_SIZE;
  }
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_ == -1;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  std::shared_lock<std::shared_mutex> lock(mtx_);
  // std::cout << 333 << std::endl;
  page_id_t tmp = GetRootPageId();
  if (tmp == INVALID_PAGE_ID) {
    return false;
  }
  while (true) {
    auto kp = bpm_->FetchPageRead(tmp);
    auto page = kp.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page->IsLeafPage()) {
      break;
    }
    page_id_t res = page->Searchkey(key, comparator_);
    tmp = res;
  }
  auto kp = bpm_->FetchPageRead(tmp);
  auto page = kp.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  bool res = page->Searchkey(key, comparator_, result);
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  std::unique_lock<std::shared_mutex> lock(mtx_);
  // std::cout << 111 << std::endl;
  page_id_t tmp = GetRootPageId();
  if (tmp == INVALID_PAGE_ID) {
    page_id_t t;
    auto res = bpm_->NewPageGuarded(&t);
    {
      WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
      auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
      root_page->root_page_id_ = t;
    }
    auto leaf_page = res.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    leaf_page->Init(leaf_max_size_);
    leaf_page->Setarray(key, value);
    leaf_page->IncreaseSize(1);
    return true;
  }
  while (true) {
    auto kp1 = bpm_->FetchPageRead(tmp);
    auto page1 = kp1.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page1->IsLeafPage()) {
      break;
    }
    page_id_t res = page1->Searchkey(key, comparator_);
    tmp = res;
  }
  auto kp1 = bpm_->FetchPageWrite(tmp);
  auto page1 = kp1.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  page_id_t ress = page1->SearchKkey(key, comparator_);
  if (comparator_(key, page1->KeyAt(ress)) == 0) {
    return false;
  }
  kp1.Drop();
  tmp = GetRootPageId();
  while (true) {
    auto kp = bpm_->FetchPageWrite(tmp);
    auto page = kp.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page->IsLeafPage()) {
      break;
    }
    page_id_t res = page->Searchkey(key, comparator_);
    tmp = res;
  }
  auto x = bpm_->FetchPageWrite(tmp);
  auto leaf_page = x.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  auto key_1 = leaf_page->KeyAt(0);
  leaf_page->Insert(key, value, comparator_);
  int a = leaf_page->GetSize();
  int b = leaf_page->GetMaxSize();
  x.Drop();
  Updatezero(tmp, key_1, 0);
  if (a == b) {
    Leafspilt(tmp);
  }
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Updatezero(bustub::page_id_t pageid, KeyType key, int ch) {
  auto x = bpm_->FetchPageWrite(pageid);
  if (ch == 0) {
    auto leaf_page = x.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    if (leaf_page->GetFather() == -1 || leaf_page->GetSize() == 0) {
      return;
    }
    auto key_1 = leaf_page->KeyAt(0);
    if (comparator_(key_1, key) != 0) {
      auto tmp = leaf_page->GetFather();
      auto father = bpm_->FetchPageWrite(tmp);
      auto leaf_1 = father.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      auto keyy = leaf_1->KeyAt(0);
      leaf_1->Delete(key, comparator_);
      leaf_1->Insert(key_1, pageid, comparator_);
      father.Drop();
      Updatezero(tmp, keyy, 1);
    }
  } else {
    auto leaf_page = x.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (leaf_page->GetFather() == -1 || leaf_page->GetSize() == 0) {
      return;
    }
    auto key_1 = leaf_page->KeyAt(0);
    if (comparator_(key_1, key) != 0) {
      auto tmp = leaf_page->GetFather();
      auto father = bpm_->FetchPageWrite(tmp);
      auto leaf_1 = father.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      auto keyy = leaf_1->KeyAt(0);
      leaf_1->Delete(key, comparator_);
      leaf_1->Insert(key_1, pageid, comparator_);
      father.Drop();
      Updatezero(tmp, keyy, 1);
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetFather(bustub::page_id_t page) {
  auto x = bpm_->FetchPageWrite(page);
  auto leaf_page = x.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  int size = leaf_page->GetSize();
  for (int i = 0; i < size; i++) {
    page_id_t tmp = leaf_page->ValueAt(i);
    auto y = bpm_->FetchPageWrite(tmp);
    auto leaf = y.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    leaf->SetFather(page);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Leafspilt(page_id_t pageid) {
  auto x = bpm_->FetchPageWrite(pageid);
  auto leaf_page = x.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  page_id_t t = 0;
  auto res = bpm_->NewPageGuarded(&t);
  auto leaf_page1 = res.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  leaf_page1->Init(leaf_max_size_);
  auto key = leaf_page->Spilt(leaf_page1);
  auto key1 = leaf_page->KeyAt(0);
  {
    auto tmp = leaf_page->GetNextPageId();
    if (tmp != -1) {
      auto y = bpm_->FetchPageWrite(tmp);
      auto leaf = y.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      leaf->SetPrvPageId(t);
    }
  }
  leaf_page1->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page1->SetPrvPageId(pageid);
  leaf_page->SetNextPageId(t);
  page_id_t internal = 0;
  if (leaf_page->GetFather() == -1) {
    auto new_internal = bpm_->NewPageGuarded(&internal);
    auto internal_page = new_internal.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    leaf_page1->SetFather(internal);
    leaf_page->SetFather(internal);
    internal_page->Init(internal_max_size_);
    internal_page->SetValueAt(0, pageid);
    internal_page->SetValueAt(1, t);
    internal_page->SetKeyAt(1, key);
    internal_page->SetKeyAt(0, key1);
    internal_page->IncreaseSize(2);
    {
      WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
      auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
      root_page->root_page_id_ = internal;
    }
  } else {
    page_id_t tmp = leaf_page->GetFather();
    leaf_page1->SetFather(tmp);
    x.Drop();
    Internalspilt(tmp, t, key);
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Internalspilt(page_id_t pageid, page_id_t son, KeyType &key) {
  auto x = bpm_->FetchPageWrite(pageid);
  auto leaf_page = x.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  if (leaf_page->GetSize() == leaf_page->GetMaxSize()) {
    page_id_t t = 0;
    auto res = bpm_->NewPageGuarded(&t);
    auto internal_page1 = res.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    internal_page1->Init(internal_max_size_);
    leaf_page->Insert(key, son, comparator_);
    auto key1 = leaf_page->Spilt(internal_page1);
    auto key2 = leaf_page->KeyAt(0);
    {
      auto tmp = leaf_page->GetNextPageId();
      if (tmp != -1) {
        auto y = bpm_->FetchPageWrite(tmp);
        auto leaf = y.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
        leaf->SetPrvPageId(t);
      }
    }
    internal_page1->SetPrvPageId(pageid);
    internal_page1->SetNextPageId(leaf_page->GetNextPageId());
    leaf_page->SetNextPageId(t);
    res.Drop();
    SetFather(t);
    auto res1 = bpm_->FetchPageWrite(t);
    auto internal_page11 = res1.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    page_id_t internal = 0;
    if (leaf_page->GetFather() == -1) {
      auto new_internal = bpm_->NewPageGuarded(&internal);
      auto internal_page = new_internal.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      internal_page11->SetFather(internal);
      leaf_page->SetFather(internal);
      internal_page->Init(internal_max_size_);
      internal_page->SetValueAt(0, pageid);
      internal_page->SetValueAt(1, t);
      internal_page->SetKeyAt(1, key1);
      internal_page->SetKeyAt(0, key2);
      internal_page->IncreaseSize(2);
      {
        WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
        auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
        root_page->root_page_id_ = internal;
      }
    } else {
      page_id_t tmp = leaf_page->GetFather();
      internal_page11->SetFather(tmp);
      res1.Drop();
      x.Drop();
      Internalspilt(tmp, t, key1);
    }
  } else {
    auto key_1 = leaf_page->KeyAt(0);
    leaf_page->Insert(key, son, comparator_);
    x.Drop();
    Updatezero(pageid, key_1, 1);
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.---+
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  std::unique_lock<std::shared_mutex> lock(mtx_);
  // std::cout << 222 << std::endl;
  page_id_t tmp = GetRootPageId();
  if (tmp == INVALID_PAGE_ID) {
    return;
  }
  while (true) {
    auto kp1 = bpm_->FetchPageRead(tmp);
    auto page1 = kp1.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page1->IsLeafPage()) {
      break;
    }
    page_id_t res = page1->Searchkey(key, comparator_);
    tmp = res;
  }
  auto kp1 = bpm_->FetchPageWrite(tmp);
  auto page1 = kp1.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  page_id_t ress = page1->SearchKkey(key, comparator_);
  if (comparator_(key, page1->KeyAt(ress)) != 0) {
    return;
  }
  kp1.Drop();
  tmp = GetRootPageId();
  while (true) {
    auto kp = bpm_->FetchPageRead(tmp);
    auto page = kp.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page->IsLeafPage()) {
      break;
    }
    page_id_t res = page->Searchkey(key, comparator_);
    tmp = res;
  }
  auto kp = bpm_->FetchPageWrite(tmp);
  auto page = kp.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  auto keyy = page->KeyAt(0);
  page->Delete(key, comparator_);
  if (tmp == GetRootPageId()) {
    if (page->GetSize() == 0) {
      WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
      auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
      root_page->root_page_id_ = -1;
    }
    return;
  }
  if (page->GetSize() == 0) {
    {
      auto ipage = page->GetNextPageId();
      if (ipage != -1) {
        auto inter = bpm_->FetchPageWrite(ipage);
        auto in_page = inter.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
        in_page->SetPrvPageId(page->GetPrvPageId());
      }
    }
    {
      auto ipage = page->GetPrvPageId();
      if (ipage != -1) {
        auto inter = bpm_->FetchPageWrite(ipage);
        auto in_page = inter.template AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
        in_page->SetNextPageId(page->GetNextPageId());
      }
    }
    auto ipage = page->GetFather();
    auto inter = bpm_->FetchPageWrite(ipage);
    auto in_page = inter.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto key1 = in_page->KeyAt(0);
    in_page->Delete(keyy, comparator_);
    auto key2 = in_page->KeyAt(0);
    int a = in_page->GetSize();
    int b = in_page->GetMinSize();
    inter.Drop();
    Updatezero(ipage, key1, 1);
    if (a < b) {
      Internalmerge(ipage, key2);
    }
  } else {
    auto key1 = page->KeyAt(0);
    int a = page->GetSize();
    int b = page->GetMinSize();
    kp.Drop();
    Updatezero(tmp, keyy, 0);
    if (a < b) {
      Leafmerge(tmp, key1);
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Leafmerge(page_id_t pageid, KeyType keyy) {
  if (pageid == GetRootPageId()) {
    return;
  }
  auto kp = bpm_->FetchPageWrite(pageid);
  auto page = kp.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  page_id_t next = page->GetNextPageId();
  if (next != INVALID_PAGE_ID) {
    auto leaf = bpm_->FetchPageWrite(next);
    auto page1 = leaf.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    if (page1->GetSize() > page1->GetMinSize()) {
      auto key = page1->KeyAt(0);
      auto value = page1->ValueAt(0);
      page1->Delete(key, comparator_);
      leaf.Drop();
      page->Setpoint(key, value, page->GetSize());
      Updatezero(next, key, 0);
      kp.Drop();
      return;
    }
    auto key = page1->KeyAt(0);
    for (int i = 0; i < page1->GetSize(); i++) {
      auto key1 = page1->KeyAt(i);
      auto value = page1->ValueAt(i);
      page->Setpoint(key1, value, page->GetSize());
    }
    auto k = page1->GetNextPageId();
    if (k != -1) {
      auto m = bpm_->FetchPageWrite(k);
      auto j = m.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      j->SetPrvPageId(pageid);
    }
    page->SetNextPageId(k);
    auto ipage = page1->GetFather();
    leaf.Drop();
    auto inter = bpm_->FetchPageWrite(ipage);
    auto in_page = inter.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto key1 = in_page->KeyAt(0);
    in_page->Delete(key, comparator_);
    auto key2 = in_page->KeyAt(0);
    int a = in_page->GetSize();
    int b = in_page->GetMinSize();
    inter.Drop();
    Updatezero(ipage, key1, 1);
    kp.Drop();
    if (a < b) {
      Internalmerge(ipage, key2);
    }
    return;
  }
  next = page->GetPrvPageId();
  if (next != INVALID_PAGE_ID) {
    auto leaf = bpm_->FetchPageWrite(next);
    auto page1 = leaf.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    if (page1->GetSize() > page1->GetMinSize()) {
      auto key = page1->KeyAt(page1->GetSize() - 1);
      auto value = page1->ValueAt(page1->GetSize() - 1);
      auto key1 = page->KeyAt(0);
      page1->Delete(key, comparator_);
      page->Insert(key, value, comparator_);
      kp.Drop();
      Updatezero(pageid, key1, 0);
      // Set_Father(page_id);
      return;
    }
    for (int i = 0; i < page->GetSize(); i++) {
      auto key1 = page->KeyAt(i);
      auto value = page->ValueAt(i);
      page1->Setpoint(key1, value, page1->GetSize());
    }
    auto k = page->GetNextPageId();
    if (k != -1) {
      auto m = bpm_->FetchPageWrite(k);
      auto j = m.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      j->SetPrvPageId(next);
    }
    page1->SetNextPageId(k);
    leaf.Drop();
    // Set_Father(page1->GetFather());
    auto ipage = page->GetFather();
    kp.Drop();
    auto inter = bpm_->FetchPageWrite(ipage);
    auto in_page = inter.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto key1 = in_page->KeyAt(0);
    in_page->Delete(keyy, comparator_);
    auto key2 = in_page->KeyAt(0);
    int a = in_page->GetSize();
    int b = in_page->GetMinSize();
    inter.Drop();
    Updatezero(ipage, key1, 1);
    if (a < b) {
      Internalmerge(ipage, key2);
    }
  }
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Internalmerge(bustub::page_id_t pageid, KeyType keyy) {
  auto kp = bpm_->FetchPageWrite(pageid);
  auto page = kp.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  if (pageid == GetRootPageId()) {
    if (page->GetSize() == 1) {
      auto tmp = page->ValueAt(0);
      auto leaf = bpm_->FetchPageWrite(tmp);
      auto leaf_page = leaf.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      leaf_page->SetFather(-1);
      {
        WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
        auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
        root_page->root_page_id_ = tmp;
      }
      kp.Drop();
    }
    return;
  }
  page_id_t next = page->GetPrvPageId();
  if (next != INVALID_PAGE_ID) {
    auto leaf = bpm_->FetchPageWrite(next);
    auto page1 = leaf.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page1->GetSize() > page1->GetMinSize()) {
      auto key = page1->KeyAt(page1->GetSize() - 1);
      auto value = page1->ValueAt(page1->GetSize() - 1);
      auto key1 = page->KeyAt(0);
      page1->Delete(key, comparator_);
      page->Insert(key, value, comparator_);
      kp.Drop();
      Updatezero(pageid, key1, 1);
      SetFather(pageid);
      return;
    }
    for (int i = 0; i < page->GetSize(); i++) {
      auto key1 = page->KeyAt(i);
      auto value = page->ValueAt(i);
      page1->Setpoint(key1, value, page1->GetSize());
    }
    auto k = page->GetNextPageId();
    if (k != -1) {
      auto m = bpm_->FetchPageWrite(k);
      auto j = m.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      j->SetPrvPageId(next);
    }
    page1->SetNextPageId(k);
    leaf.Drop();
    SetFather(next);
    auto ipage = page->GetFather();
    kp.Drop();
    auto inter = bpm_->FetchPageWrite(ipage);
    auto in_page = inter.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto key1 = in_page->KeyAt(0);
    in_page->Delete(keyy, comparator_);
    auto key2 = in_page->KeyAt(0);
    int a = in_page->GetSize();
    int b = in_page->GetMinSize();
    inter.Drop();
    Updatezero(ipage, key1, 1);
    if (a < b) {
      Internalmerge(ipage, key2);
    }
    return;
  }
  next = page->GetNextPageId();
  if (next != INVALID_PAGE_ID) {
    auto leaf = bpm_->FetchPageWrite(next);
    auto page1 = leaf.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page1->GetSize() > page1->GetMinSize()) {
      auto key = page1->KeyAt(0);
      auto value = page1->ValueAt(0);
      page1->Delete(key, comparator_);
      leaf.Drop();
      Updatezero(next, key, 1);
      page->Setpoint(key, value, page->GetSize());
      kp.Drop();
      SetFather(pageid);
      return;
    }
    auto key = page1->KeyAt(0);
    for (int i = 0; i < page1->GetSize(); i++) {
      auto key1 = page1->KeyAt(i);
      auto value = page1->ValueAt(i);
      page->Setpoint(key1, value, page->GetSize());
    }
    auto k = page1->GetNextPageId();
    if (k != -1) {
      auto m = bpm_->FetchPageWrite(k);
      auto j = m.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
      j->SetPrvPageId(pageid);
    }
    page->SetNextPageId(k);
    auto ipage = page1->GetFather();
    leaf.Drop();
    auto inter = bpm_->FetchPageWrite(ipage);
    auto in_page = inter.template AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    auto key1 = in_page->KeyAt(0);
    in_page->Delete(key, comparator_);
    auto key2 = in_page->KeyAt(0);
    int a = in_page->GetSize();
    int b = in_page->GetMinSize();
    inter.Drop();
    Updatezero(ipage, key1, 1);
    kp.Drop();
    SetFather(pageid);
    if (a < b) {
      Internalmerge(ipage, key2);
    }
    return;
  }
}
/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  page_id_t tmp = GetRootPageId();
  if (tmp == -1) {
    return INDEXITERATOR_TYPE(bpm_, -1, -1);
  }
  auto root = bpm_->FetchPageRead(tmp);
  auto root_page = root.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  auto key = root_page->KeyAt(0);
  root.Drop();
  while (true) {
    auto kp = bpm_->FetchPageRead(tmp);
    auto page = kp.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page->IsLeafPage()) {
      break;
    }
    page_id_t res = page->Searchkey(key, comparator_);
    tmp = res;
  }
  return INDEXITERATOR_TYPE(bpm_, tmp, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::unique_lock<std::shared_mutex> lock(mtx_);
  page_id_t tmp = GetRootPageId();
  if (tmp == -1) {
    return INDEXITERATOR_TYPE(bpm_, -1, -1);
  }
  while (true) {
    auto kp = bpm_->FetchPageRead(tmp);
    auto page = kp.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    if (page->IsLeafPage()) {
      break;
    }
    page_id_t res = page->Searchkey(key, comparator_);
    tmp = res;
  }
  auto root = bpm_->FetchPageRead(tmp);
  auto root_page = root.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  int ans = 0;
  for (int i = 0; i < root_page->GetSize(); i++) {
    if (comparator_(key, root_page->KeyAt(i)) == 0) {
      ans = i;
      break;
    }
  }
  return INDEXITERATOR_TYPE(bpm_, tmp, ans);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(bpm_, -1, -1); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  auto guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetRootPageId(bustub::page_id_t tmp) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = tmp;
}
/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
