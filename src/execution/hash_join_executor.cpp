//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"
namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  flag_ = false;
  ma_.clear();
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    auto key = GetHashKey(plan_, plan_->RightJoinKeyExpressions(), tuple);
    InsertMap(key, tuple);
  }
  left_executor_->Init();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &left_table_schema = plan_->GetLeftPlan()->OutputSchema();
  auto &right_table_schema = plan_->GetRightPlan()->OutputSchema();
  Tuple right;
  RID ridd;
  while (true) {
    if (tmp_.has_value() && cur_size_ < static_cast<int>(tmp_.value().size())) {
      right = tmp_.value()[cur_size_];
      cur_size_++;
      OutputTuple(left_table_schema, right_table_schema, &left_, &right, tuple, true);
      return true;
    }
    if (plan_->GetJoinType() == JoinType::LEFT && cur_size_ == 0 && flag_) {
      OutputTuple(left_table_schema, right_table_schema, &left_, &right, tuple, false);
      flag_ = false;
      return true;
    }
    cur_size_ = 0;
    auto res = left_executor_->Next(&left_, &ridd);
    if (!res) {
      return false;
    }
    auto key = GetHashKey(plan_, plan_->LeftJoinKeyExpressions(), left_);
    tmp_ = FindHash(key);
    flag_ = true;
  }
}
void HashJoinExecutor::OutputTuple(const Schema &left_table_schema, const Schema &right_table_schema,
                                   Tuple *left_tuple_, Tuple *right_tuple_, Tuple *tuple, bool matched) {
  std::vector<Value> new_tuple_values;
  for (uint32_t i = 0; i < left_table_schema.GetColumnCount(); ++i) {
    new_tuple_values.emplace_back(left_tuple_->GetValue(&left_table_schema, i));
  }

  if (!matched) {
    for (uint32_t i = 0; i < right_table_schema.GetColumnCount(); ++i) {
      auto type_id = right_table_schema.GetColumn(i).GetType();
      new_tuple_values.emplace_back(ValueFactory::GetNullValueByType(type_id));
    }
  } else {
    for (uint32_t i = 0; i < right_table_schema.GetColumnCount(); ++i) {
      new_tuple_values.emplace_back(right_tuple_->GetValue(&right_table_schema, i));
    }
  }

  *tuple = Tuple{new_tuple_values, &GetOutputSchema()};
}
}  // namespace bustub
