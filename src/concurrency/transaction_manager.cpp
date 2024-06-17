//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <mutex>  // NOLINT
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "common/macros.h"
#include "storage/table/table_heap.h"
namespace bustub {

void TransactionManager::Commit(Transaction *txn) {
  // Release all the locks.
  ReleaseLocks(txn);

  txn->SetState(TransactionState::COMMITTED);
}

void TransactionManager::Abort(Transaction *txn) {
  /* TODO: revert all the changes in write set */
  while (!txn->GetWriteSet()->empty()) {
    auto record = txn->GetWriteSet()->back();
    switch (record.wtype_) {
      case WType::INSERT: {
        TupleMeta meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = true;
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
      } break;
      case WType::DELETE: {  // 删除并没有真实删除，空洞保留，因而回退只需要更新meta即可
        TupleMeta meta = record.table_heap_->GetTupleMeta(record.rid_);
        meta.is_deleted_ = false;
        record.table_heap_->UpdateTupleMeta(meta, record.rid_);
      } break;
      default: {                                                       // UPDATE
        TupleMeta old_meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};  // 更新不会删除，更新也不会用来处理被删除的tuple
        bool success{false};
        for (auto &index_record : *txn->GetIndexWriteSet()) {
          if (index_record.table_oid_ == record.tid_ && index_record.rid_ == record.rid_) {
            record.table_heap_->UpdateTupleInPlaceUnsafe(old_meta, index_record.old_tuple_, record.rid_);
            success = true;
            break;
          }
        }
        if (!success) {
          BUSTUB_ENSURE(1 == 2, "revert update failed");
        }
      } break;
    }
    txn->GetWriteSet()->pop_back();
  }
  while (!txn->GetIndexWriteSet()->empty()) {
    auto record = txn->GetIndexWriteSet()->back();
    auto index = record.catalog_->GetIndex(record.index_oid_);
    auto tbl_info = record.catalog_->GetTable(record.table_oid_);
    auto key = record.tuple_.KeyFromTuple(tbl_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
    switch (record.wtype_) {
      case WType::INSERT: {
        index->index_->DeleteEntry(key, record.rid_, txn);
      } break;
      case WType::DELETE: {
        index->index_->InsertEntry(key, record.rid_, txn);
      } break;
      default: {  // UPDATE
        index->index_->DeleteEntry(key, record.rid_, txn);
        auto old_key =
            record.old_tuple_.KeyFromTuple(tbl_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(old_key, record.rid_, txn);
      } break;
    }
  }
  ReleaseLocks(txn);
  txn->SetState(TransactionState::ABORTED);
}

void TransactionManager::BlockAllTransactions() { UNIMPLEMENTED("block is not supported now!"); }

void TransactionManager::ResumeTransactions() { UNIMPLEMENTED("resume is not supported now!"); }

}  // namespace bustub
