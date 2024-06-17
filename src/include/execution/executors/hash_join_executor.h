//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "container/hash/hash_function.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
namespace bustub {
struct HashKey {
  /** The group-by values */
  std::vector<Value> hash_;

  /**
   * Compares two aggregate keys for equality.
   * @param other the other aggregate key to be compared with
   * @return `true` if both aggregate keys have equivalent group-by expressions, `false` otherwise
   */
  auto operator==(const HashKey &other) const -> bool {
    for (uint32_t i = 0; i < other.hash_.size(); i++) {
      if (hash_[i].CompareEquals(other.hash_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub
namespace std {
/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::HashKey> {
  auto operator()(const bustub::HashKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.hash_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};
}  // namespace std
namespace bustub {
struct TupleBucket {
  std::vector<Tuple> tuple_bucket_;
};
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };
  void OutputTuple(const Schema &left_table_schema, const Schema &right_table_schema, Tuple *left_tuple_,
                   Tuple *right_tuple_, Tuple *tuple, bool matched);
  auto InsertMap(HashKey &hash, Tuple &tuple) {
    if (ma_.count(hash) != 0U) {
      ma_[hash].tuple_bucket_.emplace_back(tuple);
      return;
    }
    ma_[hash] = TupleBucket{{tuple}};
  }
  auto FindHash(HashKey &hash) -> std::optional<std::vector<Tuple>> {
    if (ma_.count(hash) != 0U) {
      return ma_[hash].tuple_bucket_;
    }
    return std::nullopt;
  }
  auto GetHashKey(const HashJoinPlanNode *plan, const std::vector<AbstractExpressionRef> &res, Tuple &tuple)
      -> HashKey {
    HashKey tmpp;
    for (auto &x : res) {
      auto key = x->Evaluate(&tuple, plan->OutputSchema());
      tmpp.hash_.emplace_back(key);
    }
    return tmpp;
  }

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  std::unordered_map<HashKey, TupleBucket> ma_;
  int cur_size_ = 0;
  bool flag_ = false;
  Tuple tuplee_;
  std::optional<std::vector<Tuple>> tmp_;
  Tuple left_;
};

}  // namespace bustub
