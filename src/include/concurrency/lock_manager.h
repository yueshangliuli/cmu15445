//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.h
//
// Identification: src/include/concurrency/lock_manager.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <condition_variable>  // NOLINT
#include <list>
#include <map>
#include <memory>
#include <mutex>  // NOLINT
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace bustub {

class TransactionManager;

/**
 * LockManager handles transactions asking for locks on records.
 */
class LockManager {
 public:
  enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE, SHARED_INTENTION_EXCLUSIVE };

  /**
   * Structure to hold a lock request.
   * This could be a lock request on a table OR a row.
   * For table lock requests, the rid_ attribute would be unused.
   */
  class LockRequest {
   public:
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid) /** Table lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid) {}
    LockRequest(txn_id_t txn_id, LockMode lock_mode, table_oid_t oid, RID rid) /** Row lock request */
        : txn_id_(txn_id), lock_mode_(lock_mode), oid_(oid), rid_(rid) {}

    /** Txn_id of the txn requesting the lock */
    txn_id_t txn_id_;
    /** Locking mode of the requested lock */
    LockMode lock_mode_;
    /** Oid of the table for a table lock; oid of the table the row belong to for a row lock */
    table_oid_t oid_;
    /** Rid of the row for a row lock; unused for table locks */
    RID rid_;
    /** Whether the lock has been granted or not */
    bool granted_{false};
    auto operator==(const LockRequest &other) const -> bool {
      return (txn_id_ == other.txn_id_ && lock_mode_ == other.lock_mode_ && oid_ == other.oid_ && rid_ == other.rid_);
    }
  };

  class LockRequestQueue {
   public:
    /** List of lock requests for the same resource (table or row) */
    std::list<std::shared_ptr<LockRequest>> request_queue_;
    /** For notifying blocked transactions on this rid */
    std::condition_variable cv_;
    /** txn_id of an upgrading transaction (if any) */
    txn_id_t upgrading_ = INVALID_TXN_ID;
    /** coordination */
    std::mutex latch_;
  };

  /**
   * Creates a new lock manager configured for the deadlock detection policy.
   */
  LockManager() = default;

  void StartDeadlockDetection() {
    BUSTUB_ENSURE(txn_manager_ != nullptr, "txn_manager_ is not set.")
    enable_cycle_detection_ = true;
    cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
  }

  ~LockManager() {
    UnlockAll();

    enable_cycle_detection_ = false;

    if (cycle_detection_thread_ != nullptr) {
      cycle_detection_thread_->join();
      delete cycle_detection_thread_;
    }
  }

  /**
   * Acquire a lock on table_oid_t in the given lock_mode.
   * If the transaction already holds a lock on the table, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table to be locked in lock_mode
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) noexcept(false) -> bool;

  /**
   * Release the lock held on a table by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param oid the table_oid_t of the table to be unlocked
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool;

  /**
   * Acquire a lock on rid in the given lock_mode.
   * If the transaction already holds a lock on the row, upgrade the lock
   * to the specified lock_mode (if possible).
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [LOCK_NOTE] in header file.
   *
   * @param txn the transaction requesting the lock upgrade
   * @param lock_mode the lock mode for the requested lock
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be locked
   * @return true if the upgrade is successful, false otherwise
   */
  auto LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool;

  /**
   * Release the lock held on a row by the transaction.
   *
   * This method should abort the transaction and throw a
   * TransactionAbortException under certain circumstances.
   * See [UNLOCK_NOTE] in header file.
   *
   * @param txn the transaction releasing the lock
   * @param rid the RID that is locked by the transaction
   * @param oid the table_oid_t of the table the row belongs to
   * @param rid the RID of the row to be unlocked
   * @param force unlock the tuple regardless of isolation level, not changing the transaction state
   * @return true if the unlock is successful, false otherwise
   */
  auto UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force = false) -> bool;

  /*** Graph API ***/

  /**
   * Adds an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto AddEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Removes an edge from t1 -> t2 from waits for graph.
   * @param t1 transaction waiting for a lock
   * @param t2 transaction being waited for
   */
  auto RemoveEdge(txn_id_t t1, txn_id_t t2) -> void;

  /**
   * Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
   * @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
   * @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
   */
  auto HasCycle(txn_id_t *txn_id) -> bool;

  /**
   * @return all edges in current waits_for graph
   */
  auto GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>>;

  /**
   * Runs cycle detection in the background.
   */
  auto RunCycleDetection() -> void;

  TransactionManager *txn_manager_;

 private:
  /** Spring 2023 */
  /* You are allowed to modify all functions below. */
  auto Returnfail(txn_id_t txn_id, AbortReason abort_reason) -> void;
  auto GrantLock(std::list<std::shared_ptr<LockRequest>> &request_queue, LockMode &lock_mode, txn_id_t txn_id,
                 txn_id_t &upgrading) -> bool;
  auto Leagal(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool;
  auto InsertTn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void;
  auto InsertTnn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, RID rid) -> void;
  auto DeleteTn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> void;
  auto DeleteTnn(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, RID rid) -> void;
  void UnlockAll();
  auto Judge(const table_oid_t &oid, Transaction *txn) -> bool {
    int a = (*txn->GetSharedRowLockSet())[oid].size();
    int b = (*txn->GetExclusiveRowLockSet())[oid].size();
    return (a + b) != 0;
  }
  auto Insert(const table_oid_t &oid, int s, Transaction *txn) -> void {
    switch (s) {
      case 0:
        txn->GetSharedTableLockSet()->insert(oid);
        break;
      case 1:
        txn->GetExclusiveTableLockSet()->insert(oid);
        break;
      case 2:
        txn->GetIntentionSharedTableLockSet()->insert(oid);
        break;
      case 3:
        txn->GetIntentionExclusiveTableLockSet()->insert(oid);
        break;
      case 4:
        txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
        break;
    }
  }
  auto Delete(const table_oid_t &oid, int s, Transaction *txn) -> void {
    switch (s) {
      case 0:
        txn->GetSharedTableLockSet()->erase(oid);
        break;
      case 1:
        txn->GetExclusiveTableLockSet()->erase(oid);
        break;
      case 2:
        txn->GetIntentionSharedTableLockSet()->erase(oid);
        break;
      case 3:
        txn->GetIntentionExclusiveTableLockSet()->erase(oid);
        break;
      case 4:
        txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
        break;
    }
  }
  auto Insertt(const table_oid_t &oid, int s, RID rid, Transaction *txn) -> void {
    switch (s) {
      case 0:
        (*txn->GetSharedRowLockSet())[oid].insert(rid);
        break;
      case 1:
        (*txn->GetExclusiveRowLockSet())[oid].insert(rid);
        break;
    }
  }
  auto Deletee(const table_oid_t &oid, int s, RID rid, Transaction *txn) -> void {
    switch (s) {
      case 0:
        (*txn->GetSharedRowLockSet())[oid].erase(rid);
        // std::cout << "delete" << std::endl;
        break;
      case 1:
        (*txn->GetExclusiveRowLockSet())[oid].erase(rid);
        break;
    }
  }
  /*auto Dfs(txn_id_t txn_id) -> bool {
    if (safe_set_.find(txn_id) != safe_set_.end()) {
      return false;
    }
    active_set_.insert(txn_id);

    auto &next_node_vector = waits_for_[txn_id];
    // std::sort(next_node_vector.begin(), next_node_vector.end());
    for (txn_id_t const next_node : next_node_vector) {
      if (active_set_.find(next_node) != active_set_.end()) {
        return true;
      }
      if (Dfs(next_node)) {
        return true;
      }
    }

    active_set_.erase(txn_id);
    safe_set_.insert(txn_id);
    return false;
  }*/
  auto Dfs(txn_id_t txn_id, std::vector<int> &res) -> bool {
    res.push_back(txn_id);
    auto tmp = waits_for_[txn_id];
    for (auto x : tmp) {
      if (std::find(res.begin(), res.end(), x) != res.end()) {
        return true;
      }
      if (Dfs(x, res)) {
        return true;
      }
    }
    res.pop_back();
    return false;
  }
  auto DeleteNode(txn_id_t txn_id) -> void;
  /** Structure that holds lock requests for a given table oid */
  std::unordered_map<table_oid_t, std::shared_ptr<LockRequestQueue>> table_lock_map_;
  /** Coordination */
  std::mutex table_lock_map_latch_;

  /** Structure that holds lock requests for a given RID */
  std::unordered_map<RID, std::shared_ptr<LockRequestQueue>> row_lock_map_;
  /** Coordination */
  std::mutex row_lock_map_latch_;

  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::set<txn_id_t>> waits_for_;
  std::mutex waits_for_latch_;
  std::set<txn_id_t> safe_set_;
  std::set<txn_id_t> txn_set_;
  std::set<txn_id_t> active_set_;
  std::unordered_map<txn_id_t, RID> map_txn_rid_;
  std::unordered_map<txn_id_t, table_oid_t> map_txn_oid_;
};

}  // namespace bustub

template <>
struct fmt::formatter<bustub::LockManager::LockMode> : formatter<std::string_view> {
  // parse is inherited from formatter<string_view>.
  template <typename FormatContext>
  auto format(bustub::LockManager::LockMode x, FormatContext &ctx) const {
    string_view name = "unknown";
    switch (x) {
      case bustub::LockManager::LockMode::EXCLUSIVE:
        name = "EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::INTENTION_EXCLUSIVE:
        name = "INTENTION_EXCLUSIVE";
        break;
      case bustub::LockManager::LockMode::SHARED:
        name = "SHARED";
        break;
      case bustub::LockManager::LockMode::INTENTION_SHARED:
        name = "INTENTION_SHARED";
        break;
      case bustub::LockManager::LockMode::SHARED_INTENTION_EXCLUSIVE:
        name = "SHARED_INTENTION_EXCLUSIVE";
        break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};
