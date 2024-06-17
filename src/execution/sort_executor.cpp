#include "execution/executors/sort_executor.h"
#include <algorithm>
namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  cur_ = 0;
  while (child_->Next(&tuple, &rid)) {
    ve_.emplace_back(tuple, rid);
  }
  auto &order_bys = plan_->GetOrderBy();
  auto &schema = child_->GetOutputSchema();
  auto sort_by = [&order_bys, &schema](std::pair<Tuple, RID> &t1, std::pair<Tuple, RID> &t2) -> bool {
    for (const auto &order_by : order_bys) {
      Value val1 = order_by.second->Evaluate(&t1.first, schema);
      Value val2 = order_by.second->Evaluate(&t2.first, schema);

      if (val1.CompareEquals(val2) == CmpBool::CmpTrue) {  // continue if they equal in this val
        continue;
      }

      return order_by.first == OrderByType::DEFAULT || order_by.first == OrderByType::ASC
                 ? val1.CompareLessThan(val2) == CmpBool::CmpTrue
                 : val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
    }
    return false;  // return false if they equal in every val (namely themself)
  };
  std::sort(ve_.begin(), ve_.end(), sort_by);
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (cur_ == static_cast<int>(ve_.size())) {
    return false;
  }
  *tuple = ve_[cur_].first;
  *rid = ve_[cur_].second;
  cur_++;
  return true;
}

}  // namespace bustub
