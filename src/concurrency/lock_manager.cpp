//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <stack>

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {
auto LockManager::Returnfail(txn_id_t txn_id, AbortReason abort_reason) -> void {
  TransactionAbortException tmp(txn_id, abort_reason);
  std::cout << tmp.GetInfo() << std::endl;
  throw TransactionAbortException(txn_id, abort_reason);
  // std::cout << tmp.GetInfo() << std::endl;
  /* try{
     throw tmp.GetInfo();
   } catch (std::string s) {

   }*/
  // return;
}
auto LockManager::GrantLock(std::list<std::shared_ptr<LockRequest>> &request_queue, LockMode &lock_mode,
                            txn_id_t txn_id, txn_id_t &upgrading) -> bool {
  // 检测之前的锁和当前的是否有冲突
  for (auto &tmp : request_queue) {
    if (tmp->granted_) {
      if (tmp->lock_mode_ == LockMode::INTENTION_SHARED && lock_mode == LockMode::EXCLUSIVE) {
        return false;
      }
      if (tmp->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
           lock_mode == LockMode::SHARED)) {
        return false;
      }
      if (tmp->lock_mode_ == LockMode::SHARED &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
           lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        return false;
      }
      if (tmp->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::INTENTION_SHARED) {
        return false;
      }
      if (tmp->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
  }
  // 升级的是自己直接返回
  if (upgrading == txn_id) {
    for (auto &tmp : request_queue) {
      if (!tmp->granted_) {
        if (tmp->txn_id_ == txn_id) {
          tmp->granted_ = true;
          upgrading = INVALID_TXN_ID;
          return true;
        }
      }
    }
  }
  // 看自己当前的优先级是不是最高
  for (auto &tmp : request_queue) {
    if (!tmp->granted_) {
      if (tmp->txn_id_ == txn_id) {
        tmp->granted_ = true;
        return true;
      }  // else {
      if (tmp->lock_mode_ == LockMode::INTENTION_SHARED && lock_mode == LockMode::EXCLUSIVE) {
        return false;
      }
      if (tmp->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
           lock_mode == LockMode::SHARED)) {
        return false;
      }
      if (tmp->lock_mode_ == LockMode::SHARED &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE ||
           lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        return false;
      }
      if (tmp->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && lock_mode != LockMode::INTENTION_SHARED) {
        return false;
      }
      if (tmp->lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    //}
  }
  return true;
}
auto LockManager::Leagal(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 在shrinking阶段有的锁是根本不能加的
  if (txn->GetState() == TransactionState::SHRINKING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_EXCLUSIVE || lock_mode == LockMode::EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        Returnfail(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
      txn->SetState(TransactionState::ABORTED);
      Returnfail(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      return false;
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        Returnfail(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
      txn->SetState(TransactionState::ABORTED);
      Returnfail(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }
  if (txn->GetState() == TransactionState::GROWING) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        txn->SetState(TransactionState::ABORTED);
        Returnfail(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        return false;
      }
    }
  }
  return true;
}
auto LockManager::InsertTn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
  // 在txn中的set里插入东西
  txn->LockTxn();
  switch (lock_mode) {
    case LockMode::SHARED:
      Insert(oid, 0, txn);
      //  std::cout << 0 << std::endl;
      break;
    case LockMode::EXCLUSIVE:
      Insert(oid, 1, txn);
      // std::cout << 1 << std::endl;
      break;
    case LockMode::INTENTION_SHARED:
      Insert(oid, 2, txn);
      //   std::cout << 2 << std::endl;
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      Insert(oid, 3, txn);
      //   std::cout << 3 << std::endl;
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      Insert(oid, 4, txn);
      //  std::cout << 4 << std::endl;
      break;
  }
  txn->UnlockTxn();
}
auto LockManager::DeleteTn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void {
  txn->LockTxn();
  switch (lock_mode) {
    case LockMode::SHARED:
      Delete(oid, 0, txn);
      // std::cout << 0 << std::endl;
      break;
    case LockMode::EXCLUSIVE:
      Delete(oid, 1, txn);
      // std::cout << 1 << std::endl;
      break;
    case LockMode::INTENTION_SHARED:
      Delete(oid, 2, txn);
      // std::cout << 2 << std::endl;
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      Delete(oid, 3, txn);
      // std::cout << 3 << std::endl;
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      Delete(oid, 4, txn);
      // std::cout << 4 << std::endl;
      break;
  }
  txn->UnlockTxn();
}
auto LockManager::DeleteTnn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, RID rid) -> void {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    Deletee(oid, 0, rid, txn);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    Deletee(oid, 1, rid, txn);
  }
  txn->UnlockTxn();
}
auto LockManager::InsertTnn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, RID rid) -> void {
  txn->LockTxn();
  if (lock_mode == LockMode::SHARED) {
    Insertt(oid, 0, rid, txn);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    Insertt(oid, 1, rid, txn);
  }
  txn->UnlockTxn();
}
auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  // 判断此时事务的请求是否合法
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    // throw std::runtime_error("0");
    return false;
  }
  if (!Leagal(txn, lock_mode, oid)) {
    return false;
  }
  // 获取加锁队列并判断有无锁升级
  row_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    std::shared_ptr<LockRequestQueue> tmp = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = tmp;
  }
  auto tmp = table_lock_map_[oid];
  row_lock_map_latch_.unlock();
  tmp->latch_.lock();
  bool flag = false;
  std::shared_ptr<LockRequest> ans;
  std::shared_ptr<LockRequest> tt;
  for (auto &res : tmp->request_queue_) {
    if (res->txn_id_ == txn->GetTransactionId() && res->granted_) {
      if (res->lock_mode_ == lock_mode) {
        tmp->latch_.unlock();
        return true;
      }
      if (tmp->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        tmp->latch_.unlock();
        Returnfail(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      if (res->lock_mode_ == LockMode::INTENTION_SHARED && (lock_mode != LockMode::INTENTION_SHARED)) {
        tt = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
        tmp->request_queue_.push_back(tt);
        ans = res;
        tmp->upgrading_ = txn->GetTransactionId();
        flag = true;
        break;
      }
      if (res->lock_mode_ == LockMode::SHARED &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        tt = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
        tmp->request_queue_.push_back(tt);
        ans = res;
        tmp->upgrading_ = txn->GetTransactionId();
        flag = true;
        break;
      }
      if (res->lock_mode_ == LockMode::INTENTION_EXCLUSIVE &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        tt = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
        tmp->request_queue_.push_back(tt);
        ans = res;
        tmp->upgrading_ = txn->GetTransactionId();
        flag = true;
        break;
      }
      if (res->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE && (lock_mode == LockMode::EXCLUSIVE)) {
        tt = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
        tmp->request_queue_.push_back(tt);
        ans = res;
        tmp->upgrading_ = txn->GetTransactionId();
        flag = true;
        break;
      }
      txn->SetState(TransactionState::ABORTED);
      tmp->latch_.unlock();
      Returnfail(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
  }
  if (!flag) {
    tt = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
    tmp->request_queue_.push_back(tt);
    tmp->latch_.unlock();
  } else {
    LockMode t = ans->lock_mode_;
    auto it = std::find(tmp->request_queue_.begin(), tmp->request_queue_.end(), ans);
    tmp->request_queue_.erase(it);
    DeleteTn(txn, t, oid);
    tmp->latch_.unlock();
    tmp->cv_.notify_all();
  }
  // 获取资源加锁
  std::unique_lock<std::mutex> lock(tmp->latch_);
  while (!GrantLock(tmp->request_queue_, lock_mode, txn->GetTransactionId(), tmp->upgrading_)) {
    tmp->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      tmp->request_queue_.remove(tt);
      tmp->cv_.notify_all();
      // throw std::runtime_error("0");
      return false;
    }
  }
  /*if(txn->GetState() == TransactionState::ABORTED) {
    auto it = std::find(tmp->request_queue_.begin(), tmp->request_queue_.end(), tt);
    if(it != tmp->request_queue_.end()) tmp->request_queue_.erase(it);
    lock.unlock();
    tmp->cv_.notify_all();
    // throw std::runtime_error("0");
    return false;
  }*/
  InsertTn(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  if (Judge(oid, txn)) {
    txn->SetState(TransactionState::ABORTED);
    Returnfail(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
    return false;
  }
  row_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    std::shared_ptr<LockRequestQueue> tmp = std::make_shared<LockRequestQueue>();
    table_lock_map_[oid] = tmp;
  }
  auto tmp = table_lock_map_[oid];
  row_lock_map_latch_.unlock();
  tmp->latch_.lock();
  bool flag = false;
  std::shared_ptr<LockRequest> ans;
  for (auto &res : tmp->request_queue_) {
    if (res->txn_id_ == txn->GetTransactionId() && res->granted_) {
      flag = true;
      ans = res;
      break;
    }
  }
  tmp->latch_.unlock();
  if (!flag) {
    std::cout << "Unlocktable" << std::endl;
    txn->SetState(TransactionState::ABORTED);
    Returnfail(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  LockMode k = ans->lock_mode_;
  tmp->latch_.lock();
  auto it = std::find(tmp->request_queue_.begin(), tmp->request_queue_.end(), ans);
  tmp->request_queue_.erase(it);
  DeleteTn(txn, k, oid);
  tmp->latch_.unlock();
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && k == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && k == LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::SHRINKING);
  }
  if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
      (k == LockMode::EXCLUSIVE || k == LockMode::SHARED)) {
    txn->SetState(TransactionState::SHRINKING);
  }
  tmp->cv_.notify_all();
  return true;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 判断此时事务的请求是否合法
  if (txn->GetState() == TransactionState::COMMITTED || txn->GetState() == TransactionState::ABORTED) {
    // throw std::runtime_error("0");
    return false;
  }
  if (!Leagal(txn, lock_mode, oid)) {
    return false;
  }
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    std::shared_ptr<LockRequestQueue> tmp = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = tmp;
  }
  auto tmp = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  tmp->latch_.lock();
  bool flag = false;
  std::shared_ptr<LockRequest> ans;
  std::shared_ptr<LockRequest> tt;
  for (auto &res : tmp->request_queue_) {
    if (res->txn_id_ == txn->GetTransactionId() && res->granted_) {
      if (res->lock_mode_ == lock_mode) {
        tmp->latch_.unlock();
        return true;
      }
      if (tmp->upgrading_ != INVALID_TXN_ID) {
        txn->SetState(TransactionState::ABORTED);
        tmp->latch_.unlock();
        Returnfail(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      if (res->lock_mode_ == LockMode::SHARED && (lock_mode == LockMode::EXCLUSIVE)) {
        tt = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
        tmp->request_queue_.push_back(tt);
        ans = res;
        tmp->upgrading_ = txn->GetTransactionId();
        flag = true;
        break;
      }  // else {
      txn->SetState(TransactionState::ABORTED);
      tmp->latch_.unlock();
      Returnfail(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
      return false;
    }
    // }
  }
  if (!flag) {
    tt = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
    tmp->request_queue_.push_back(tt);
    tmp->latch_.unlock();
  } else {
    LockMode t = ans->lock_mode_;
    auto it = std::find(tmp->request_queue_.begin(), tmp->request_queue_.end(), ans);
    tmp->request_queue_.erase(it);
    DeleteTnn(txn, t, oid, rid);
    tmp->latch_.unlock();
    tmp->cv_.notify_all();
  }
  row_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0U) {
    std::shared_ptr<LockRequestQueue> t;
    table_lock_map_[oid] = t;
  }
  auto t = table_lock_map_[oid];
  row_lock_map_latch_.unlock();
  t->latch_.lock();
  bool f = false;
  for (auto &res : t->request_queue_) {
    if (res->oid_ == oid && res->granted_) {
      if (lock_mode == LockMode::SHARED) {
        f = true;
        break;
      }
      if (lock_mode == LockMode::EXCLUSIVE &&
          (res->lock_mode_ == LockMode::INTENTION_EXCLUSIVE || res->lock_mode_ == LockMode::EXCLUSIVE ||
           res->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE)) {
        f = true;
        break;
      }
    }
  }
  t->latch_.unlock();
  if (!f) {
    txn->SetState(TransactionState::ABORTED);
    Returnfail(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  std::unique_lock<std::mutex> lock(tmp->latch_);
  while (!GrantLock(tmp->request_queue_, lock_mode, txn->GetTransactionId(), tmp->upgrading_)) {
    tmp->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      tmp->request_queue_.remove(tt);
      tmp->cv_.notify_all();
      // throw std::runtime_error("0");
      return false;
    }
  }
  /*if(txn->GetState() == TransactionState::ABORTED) {
    tmp->latch_.lock();
    for(auto res: tmp->request_queue_) {
      if(res->txn_id_ == txn->GetTransactionId() && res->granted_) {
        res->granted_ = false;
        break;
      }
    }
    tmp->latch_.unlock();
    tmp->cv_.notify_all();
    return false;
  }*/
  InsertTnn(txn, lock_mode, oid, rid);
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    std::shared_ptr<LockRequestQueue> tmp = std::make_shared<LockRequestQueue>();
    row_lock_map_[rid] = tmp;
  }
  auto tmp = row_lock_map_[rid];
  row_lock_map_latch_.unlock();
  tmp->latch_.lock();
  bool flag = false;
  std::shared_ptr<LockRequest> ans;
  for (auto &res : tmp->request_queue_) {
    if (res->txn_id_ == txn->GetTransactionId() && res->granted_) {
      flag = true;
      ans = res;
      break;
    }
  }
  tmp->latch_.unlock();
  if (!flag) {
    std::cout << "Unlockrow" << std::endl;
    txn->SetState(TransactionState::ABORTED);
    Returnfail(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
    return false;
  }
  LockMode k = ans->lock_mode_;
  tmp->latch_.lock();
  auto it = std::find(tmp->request_queue_.begin(), tmp->request_queue_.end(), ans);
  tmp->request_queue_.erase(it);
  DeleteTnn(txn, k, oid, rid);
  tmp->latch_.unlock();
  if (!force) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && k == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && k == LockMode::EXCLUSIVE) {
      txn->SetState(TransactionState::SHRINKING);
    }
    if (txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
        (k == LockMode::EXCLUSIVE || k == LockMode::SHARED)) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  tmp->cv_.notify_all();
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  /* if(txn_manager_->GetTransaction(t1)->GetState() == TransactionState::ABORTED) {
     return;
   }*/
  waits_for_latch_.lock();
  /*if(txn_manager_->GetTransaction(t1)->GetState() != TransactionState::ABORTED) {*/
  txn_set_.insert(t1);
  txn_set_.insert(t2);
  waits_for_[t1].insert(t2);
  // }
  waits_for_latch_.unlock();
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_latch_.lock();
  if (waits_for_.count(t1) != 0U) {
    auto &tmp = waits_for_[t1];
    auto it = std::find(tmp.begin(), tmp.end(), t2);
    if (it != tmp.end()) {
      tmp.erase(it);
    }
  }
  waits_for_latch_.unlock();
}

/*auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  std::stack<std::pair<txn_id_t, std::vector<txn_id_t>>> st;
  if(cur_ <= max_) {
    std::vector<txn_id_t> ve;
    st.push({cur_, ve});
  } else {
    return false;
  }
  std::unordered_map<txn_id_t, int> ma;
  ma[cur_] = 1;
  while (!st.empty()) {
    auto x = st.top();
    st.pop();
    if(waits_for_.count(x.first)) {
      auto tmp = waits_for_[x.first];
      for(auto &t: tmp) {
        if(!ma.count(t)) {
          std::vector<txn_id_t> ve = x.second;
          ve.push_back(t);
          st.push({t, ve});
          ma[t] = 1;
        } else if(t == cur_) {
          txn_id_t ans = t;
          for(auto &y: x.second) {
            ans = std::max(ans, y);
          }
          *txn_id = ans;
          cur_++;
          //RemoveEdge(x.first, cur_);
          return true;
        }
      }
    }
  }
  cur_++;
  return false;
}*/
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  for (auto const &start_txn_id : txn_set_) {
    /* if (Dfs(start_txn_id)) {
       *txn_id = *active_set_.begin();
       for (auto const &active_txn_id : active_set_) {
         *txn_id = std::max(*txn_id, active_txn_id);
       }
       active_set_.clear();
       return true;
     }*/
    // active_set_.clear();
    std::vector<int> ve;
    if (Dfs(start_txn_id, ve)) {
      *txn_id = ve[0];
      for (auto x : ve) {
        *txn_id = std::max(x, *txn_id);
      }
      return true;
    }
  }
  return false;
}
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &[x, y] : waits_for_) {
    for (auto &z : y) {
      edges.emplace_back(x, z);
    }
  }
  return edges;
}
auto LockManager::DeleteNode(txn_id_t txn_id) -> void {
  waits_for_.erase(txn_id);

  for (auto a_txn_id : txn_set_) {
    if (a_txn_id != txn_id) {
      RemoveEdge(a_txn_id, txn_id);
    }
  }
}
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      /* cur_ = 0;
       max_ = 0;
       for(auto &[x, y]: table_lock_map_) {
         auto tmp = y->request_queue_;
         std::set<txn_id_t> l;
         for(auto &i: tmp) {
           if(i->granted_) {
             l.insert(i->txn_id_);
           } else {
             for(auto &j: l) {
               AddEdge(i->txn_id_, j);
             }
           }
         }
       }
       for(auto &[x, y]: row_lock_map_) {
         auto tmp = y->request_queue_;
         std::set<txn_id_t> l;
         for(auto &i: tmp) {
           if(i->granted_) {
             l.insert(i->txn_id_);
           } else {
             for(auto &j: l) {
               AddEdge(i->txn_id_, j);
             }
           }
         }
       }*/
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();
      for (auto &pair : table_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();
        for (auto const &lock_request : pair.second->request_queue_) {
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted_set) {
              map_txn_oid_.emplace(lock_request->txn_id_, lock_request->oid_);
              AddEdge(lock_request->txn_id_, txn_id);
              std::cout << "AddEdge"
                        << " " << lock_request->txn_id_ << " " << txn_id << std::endl;
            }
          }
        }
        pair.second->latch_.unlock();
      }

      for (auto &pair : row_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        pair.second->latch_.lock();
        for (auto const &lock_request : pair.second->request_queue_) {
          if (lock_request->granted_) {
            granted_set.emplace(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted_set) {
              map_txn_rid_.emplace(lock_request->txn_id_, lock_request->rid_);
              AddEdge(lock_request->txn_id_, txn_id);
              std::cout << "AddEdge"
                        << " " << lock_request->txn_id_ << " " << txn_id << std::endl;
            }
          }
        }
        pair.second->latch_.unlock();
      }
      /*auto ve = GetEdgeList();
      for(auto &[x, y]: ve) {
        std::cout << x << " " << y << std::endl;
      }*/
      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();
      txn_id_t ans;
      while (HasCycle(&ans)) {
        // std::cout << ans << std::endl;
        auto tmp = txn_manager_->GetTransaction(ans);
        tmp->SetState(TransactionState::ABORTED);
        // table_lock_map_latch_.lock();
        DeleteNode(ans);
        /* for(auto &[x, y]: table_lock_map_) {
           y->latch_.lock();
           auto z = y->request_queue_;
           for (auto it = z.begin(); it != z.end();) {
             if ((*it)->txn_id_ == ans) {
               it = z.erase(it); // 删除元素，并返回指向下一个元素的迭代器
             } else {
               ++it;
             }
           }
           y->latch_.unlock();
           y->cv_.notify_all();
         }
         for(auto &[x, y]: row_lock_map_) {
           y->latch_.lock();
           auto z = y->request_queue_;
           for (auto it = z.begin(); it != z.end();) {
             if ((*it)->txn_id_ == ans) {
               it = z.erase(it); // 删除元素，并返回指向下一个元素的迭代器
             } else {
               ++it;
             }
           }
           y->latch_.unlock();
           y->cv_.notify_all();*/
        if (map_txn_oid_.count(ans) > 0) {
          table_lock_map_[map_txn_oid_[ans]]->latch_.lock();
          table_lock_map_[map_txn_oid_[ans]]->cv_.notify_all();
          table_lock_map_[map_txn_oid_[ans]]->latch_.unlock();
        }
        if (map_txn_rid_.count(ans) > 0) {
          row_lock_map_[map_txn_rid_[ans]]->latch_.lock();
          row_lock_map_[map_txn_rid_[ans]]->cv_.notify_all();
          row_lock_map_[map_txn_rid_[ans]]->latch_.unlock();
        }
      }
      waits_for_.clear();
      safe_set_.clear();
      txn_set_.clear();
      map_txn_oid_.clear();
      map_txn_rid_.clear();
    }
  }
}

}  // namespace bustub
