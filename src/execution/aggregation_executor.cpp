//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->aggregates_, plan->agg_types_),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  aht_.Clear();
  child_->Init();
  Tuple tuple;
  RID rid;
  num_of_tuples_ = 0;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    num_of_tuples_++;
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (num_of_tuples_ == 0 && plan_->group_bys_.empty()) {  // empty table
    Tuple output_tuple(aht_.GenerateInitialAggregateValue().aggregates_, &plan_->OutputSchema());
    *tuple = output_tuple;
    *rid = output_tuple.GetRid();
    --num_of_tuples_;
    return true;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  std::vector<Value> values = aht_iterator_.Key().group_bys_;
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());
  Tuple out_tuple(values, &GetOutputSchema());
  RID ridd = out_tuple.GetRid();
  *tuple = out_tuple;
  *rid = ridd;
  ++aht_iterator_;
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
