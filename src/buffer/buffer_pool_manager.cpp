//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();
  if (free_list_.empty()) {
    frame_id_t id;
    auto flag = replacer_->Evict(&id);
    if (!flag) {
      latch_.unlock();
      return nullptr;
    }
    page_id_t x = AllocatePage();
    if (pages_[id].is_dirty_) {
      disk_scheduler_->WritePage(pages_[id].page_id_, pages_[id].data_);
      pages_[id].ResetMemory();
    }
    page_table_.erase(pages_[id].page_id_);
    replacer_->RecordAccess(id, AccessType::Unknown);
    pages_[id].page_id_ = x;
    pages_[id].is_dirty_ = false;
    pages_[id].pin_count_ = 1;
    page_table_[x] = id;
    *page_id = x;
    latch_.unlock();
    return &pages_[id];
  }
  frame_id_t id = free_list_.front();
  free_list_.pop_front();
  page_id_t x = AllocatePage();
  replacer_->RecordAccess(id, AccessType::Unknown);
  pages_[id].page_id_ = x;
  pages_[id].is_dirty_ = false;
  pages_[id].pin_count_ = 1;
  page_table_[x] = id;
  *page_id = x;
  latch_.unlock();
  return &pages_[id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();
  if (page_table_.count(page_id) == 0U) {
    if (free_list_.empty()) {
      frame_id_t id;
      auto flag = replacer_->Evict(&id);
      if (!flag) {
        latch_.unlock();
        return nullptr;
      }
      if (pages_[id].is_dirty_) {
        disk_scheduler_->WritePage(pages_[id].page_id_, pages_[id].data_);
        pages_[id].ResetMemory();
      }
      page_table_.erase(pages_[id].page_id_);
      replacer_->RecordAccess(id, AccessType::Unknown);
      pages_[id].page_id_ = page_id;
      pages_[id].is_dirty_ = false;
      pages_[id].pin_count_ = 1;
      page_table_[page_id] = id;
      disk_scheduler_->ReadPage(page_id, pages_[id].data_);
      latch_.unlock();
      return &pages_[id];
    }
    frame_id_t id = free_list_.front();
    free_list_.pop_front();
    replacer_->RecordAccess(id, AccessType::Unknown);
    pages_[id].page_id_ = page_id;
    pages_[id].is_dirty_ = false;
    pages_[id].pin_count_ = 1;
    page_table_[page_id] = id;
    disk_scheduler_->ReadPage(page_id, pages_[id].data_);
    latch_.unlock();
    return &pages_[id];
  }
  frame_id_t id = page_table_[page_id];
  if (pages_[id].pin_count_ == 0) {
    replacer_->SetEvictable(id, false);
  }
  pages_[id].pin_count_++;
  latch_.unlock();
  return &pages_[id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0U) {
    latch_.unlock();
    return false;
  }
  frame_id_t id = page_table_[page_id];
  if (!pages_[id].is_dirty_) {
    pages_[id].is_dirty_ = is_dirty;
  }
  if (pages_[id].pin_count_ <= 0) {
    latch_.unlock();
    return false;
  }
  pages_[id].pin_count_--;
  if (pages_[id].pin_count_ == 0) {
    replacer_->SetEvictable(id, true);
  }
  latch_.unlock();
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0U) {
    latch_.unlock();
    return false;
  }
  frame_id_t id = page_table_[page_id];
  disk_scheduler_->WritePage(page_id, pages_[id].data_);
  pages_[id].is_dirty_ = false;
  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (auto &[x, y] : page_table_) {
    if (pages_[y].IsDirty()) {
      FlushPage(x);
    }
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  if (page_table_.count(page_id) == 0U) {
    latch_.unlock();
    return true;
  }
  frame_id_t id = page_table_[page_id];
  if (pages_[id].pin_count_ != 0) {
    latch_.unlock();
    return false;
  }
  replacer_->Remove(id);
  free_list_.push_back(id);
  pages_[id].ResetMemory();
  pages_[id].pin_count_ = 0;
  pages_[id].is_dirty_ = false;
  page_table_.erase(page_id);
  DeallocatePage(page_id);
  latch_.unlock();
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  return BasicPageGuard{this, FetchPage(page_id)};
}

// fetch the page and put the read latch on the page
auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  page->RLatch();
  return {this, page};
}

// fetch the page and put the write latch on the page
auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page == nullptr) {
    return {this, nullptr};
  }
  page->WLatch();
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  return BasicPageGuard{this, NewPage(page_id)};
}
}  // namespace bustub
