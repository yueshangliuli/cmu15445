//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
LRUKReplacer::~LRUKReplacer() {
  for (auto &[x, y] : node_store_) {
    delete y;
  }
}
auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  if (curr_size_ != 0) {
    bool flag = false;
    auto tmp = lr_.end();
    tmp--;
    for (; tmp != lr_.begin(); tmp--) {
      if ((*tmp)->is_evictable_) {
        flag = true;
        break;
      }
    }
    if ((*tmp) != nullptr && (*tmp)->is_evictable_) {
      flag = true;
    }
    if (flag) {
      *frame_id = ((*tmp)->fid_);
      node_store_.erase((*tmp)->fid_);
      curr_size_--;
      current_timestamp_--;
      delete (*tmp);
      lr_.erase(tmp);
      latch_.unlock();
      return true;
    }
    tmp = qr_.end();
    tmp--;
    for (; tmp != qr_.begin(); tmp--) {
      if ((*tmp)->is_evictable_) {
        break;
      }
    }
    *frame_id = ((*tmp)->fid_);
    node_store_.erase((*tmp)->fid_);
    curr_size_--;
    current_timestamp_--;
    delete (*tmp);
    qr_.erase(tmp);
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  assert(current_timestamp_ <= replacer_size_);
  latch_.lock();
  if (node_store_.count(frame_id) != 0U) {
    LRUKNode *tmp = node_store_[frame_id];
    if (tmp->k_ < k_) {
      auto x = *(std::find(lr_.begin(), lr_.end(), tmp));
      tmp->k_++;
      if (tmp->k_ == k_) {
        lr_.remove(x);
        qr_.push_front(x);
      }
    } else {
      auto x = *(std::find(qr_.begin(), qr_.end(), tmp));
      tmp->k_++;
      qr_.remove((x));
      qr_.push_front(x);
    }
    latch_.unlock();
  } else {
    auto *tmp = new LRUKNode(frame_id, false, 1);
    node_store_[frame_id] = tmp;
    if (tmp->k_ < k_) {
      lr_.push_front(tmp);
    } else {
      qr_.push_front(tmp);
    }
    current_timestamp_++;
    latch_.unlock();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  if (node_store_.count(frame_id) == 0U) {
    latch_.unlock();
    throw std::out_of_range("frame_id is not found");
  }
  LRUKNode *tmp = node_store_[frame_id];
  if (tmp->is_evictable_ != set_evictable) {
    if (set_evictable) {
      tmp->is_evictable_ = true;
      curr_size_++;
    } else {
      tmp->is_evictable_ = false;
      curr_size_--;
    }
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  if (node_store_.count(frame_id) == 0U) {
    latch_.unlock();
    return;
  }
  LRUKNode *tmp = node_store_[frame_id];
  if (tmp->is_evictable_) {
    if (tmp->k_ < k_) {
      auto x = *(std::find(lr_.begin(), lr_.end(), tmp));
      lr_.remove((x));
      node_store_.erase(frame_id);
      delete (x);
      current_timestamp_--;
      curr_size_--;
      latch_.unlock();
    } else {
      auto x = *(std::find(qr_.begin(), qr_.end(), tmp));
      qr_.remove((x));
      node_store_.erase(frame_id);
      delete (x);
      current_timestamp_--;
      curr_size_--;
      latch_.unlock();
    }
  } else {
    latch_.unlock();
    return;
  }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
