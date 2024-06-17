//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  auto catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->table_oid_);
  indexes_info_ = catalog->GetTableIndexes(table_info_->name_);
  child_executor_->Init();
  outputted_ = false;
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (outputted_) {
    return false;
  }
  int nums = 0;
  while (child_executor_->Next(tuple, rid)) {
    TupleMeta tuple_meta{INVALID_TXN_ID, INVALID_TXN_ID, true};
    table_info_->table_->UpdateTupleMeta(tuple_meta, *rid);
    std::vector<Value> res;
    for (auto &tmp : plan_->target_expressions_) {
      res.emplace_back(tmp->Evaluate(tuple, table_info_->schema_));
    }
    Tuple ans{res, &table_info_->schema_};
    tuple_meta.is_deleted_ = false;
    std::optional<RID> k = table_info_->table_->InsertTuple(tuple_meta, ans, exec_ctx_->GetLockManager(),
                                                            exec_ctx_->GetTransaction(), plan_->table_oid_);
    if (!k.has_value()) {
      return false;
    }
    *rid = k.value();
    nums++;
    for (auto tmp : indexes_info_) {
      auto delete_key = tuple->KeyFromTuple(table_info_->schema_, tmp->key_schema_, tmp->index_->GetKeyAttrs());
      auto update_key = ans.KeyFromTuple(table_info_->schema_, tmp->key_schema_, tmp->index_->GetKeyAttrs());
      tmp->index_->DeleteEntry(delete_key, *rid, exec_ctx_->GetTransaction());
      if (!tmp->index_->InsertEntry(update_key, *rid, exec_ctx_->GetTransaction())) {
        return false;
      }
    }
  }
  std::vector<Value> values{{TypeId::INTEGER, nums}};
  Tuple output_tuple(values, &GetOutputSchema());
  *tuple = output_tuple;
  outputted_ = true;
  return true;
}

}  // namespace bustub
