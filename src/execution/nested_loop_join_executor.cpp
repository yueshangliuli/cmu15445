//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}
void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  left_tuple_ = new Tuple();
  right_tuple_ = new Tuple();

  RID rid;
  left_executor_->Next(left_tuple_, &rid);
  joined_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto &predicate = plan_->Predicate();
  auto &left_table_schema = plan_->GetLeftPlan()->OutputSchema();  // smaller table
  auto &right_table_schema = plan_->GetRightPlan()->OutputSchema();
  while (true) {
    while (right_executor_->Next(right_tuple_, rid)) {
      Value check_equal = predicate->EvaluateJoin(left_tuple_, left_table_schema, right_tuple_, right_table_schema);
      if (check_equal.GetAs<bool>()) {
        OutputTuple(left_table_schema, right_table_schema, tuple, true);
        return true;
      }
    }

    if (plan_->GetJoinType() == JoinType::LEFT && !joined_) {
      OutputTuple(left_table_schema, right_table_schema, tuple, false);
      right_executor_->Init();
      return true;
    }

    if (!left_executor_->Next(left_tuple_, rid)) {
      delete left_tuple_;
      left_tuple_ = nullptr;

      delete right_tuple_;
      right_tuple_ = nullptr;

      return false;
    }

    right_executor_->Init();
    joined_ = false;
  }
}

void NestedLoopJoinExecutor::OutputTuple(const Schema &left_table_schema, const Schema &right_table_schema,
                                         Tuple *tuple, bool matched) {
  std::vector<Value> new_tuple_values;
  new_tuple_values.reserve(
      GetOutputSchema().GetColumnCount());  // 提前分配好固定大小的内存空间, 避免动态扩容 (很聪明的优化)

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

  joined_ = true;
  *tuple = Tuple{new_tuple_values, &GetOutputSchema()};
}
}  // namespace bustub
