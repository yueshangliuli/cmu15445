#include <algorithm>
#include <memory>
#include <optional>
#include <tuple>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_index_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Spring: You should at least support join keys of the form:
  // 1. <column expr> = <column expr>
  // 2. <column expr> = <column expr> AND <column expr> = <column expr>

  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");

    std::vector<AbstractExpressionRef> left_exprs;
    std::vector<AbstractExpressionRef> right_exprs;

    // std::cout << plan->ToString() << std::endl << std::endl;
    // std::cout << "Predicate of the plan: " << nlj_plan.Predicate()->ToString() << std::endl << std::endl; // e.g.
    // (#0.0=#1.0)

    // 这个写法很妙
    if (const auto *expr1 = dynamic_cast<const ComparisonExpression *>(nlj_plan.Predicate().get());
        expr1 != nullptr) {  // <column expr> = <column expr>
      if (expr1->comp_type_ == ComparisonType::Equal) {
        OutputExpresions(expr1, left_exprs, right_exprs);
        return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                  nlj_plan.GetRightPlan(), left_exprs, right_exprs,
                                                  nlj_plan.GetJoinType());
      }
    } else if (const auto *expr = dynamic_cast<const LogicExpression *>(nlj_plan.Predicate().get());
               expr != nullptr) {  // <column expr> = <column expr> AND <column expr> = <column expr>
      if (expr->logic_type_ == LogicType::And) {
        if (const auto *left_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[0].get());
            left_expr != nullptr) {
          if (const auto *right_expr = dynamic_cast<const ComparisonExpression *>(expr->children_[1].get());
              right_expr != nullptr) {
            // std::cout << "left_expr: " << left_expr->ToString() << std::endl;
            // std::cout << "right_expr: " << right_expr->ToString() << std::endl;

            OutputExpresions(left_expr, left_exprs, right_exprs);
            OutputExpresions(right_expr, left_exprs, right_exprs);
            return std::make_shared<HashJoinPlanNode>(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(),
                                                      nlj_plan.GetRightPlan(), left_exprs, right_exprs,
                                                      nlj_plan.GetJoinType());
          }
        }
      }
    }
  }

  return optimized_plan;
}

void Optimizer::OutputExpresions(const ComparisonExpression *expr, std::vector<AbstractExpressionRef> &left_exprs,
                                 std::vector<AbstractExpressionRef> &right_exprs) {
  if (const auto *left_col_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[0].get());
      left_col_expr != nullptr) {
    if (const auto *right_col_expr = dynamic_cast<const ColumnValueExpression *>(expr->children_[1].get());
        right_col_expr != nullptr) {
      auto left_expr_tuple_0 =
          std::make_shared<ColumnValueExpression>(0, left_col_expr->GetColIdx(), left_col_expr->GetReturnType());
      auto right_expr_tuple_0 =
          std::make_shared<ColumnValueExpression>(0, right_col_expr->GetColIdx(), right_col_expr->GetReturnType());

      // std::cout << "right expr" << std::endl;
      // std::cout << "left_expr_tuple_0: " << left_expr_tuple_0->ToString() << std::endl;
      // std::cout << "right_expr_tuple_0: " << right_expr_tuple_0->ToString() << std::endl;

      // 这里只需要交换 expr 就可以了, 不需要交换 plan
      if (left_col_expr->GetTupleIdx() == 0 && right_col_expr->GetTupleIdx() == 1) {
        left_exprs.emplace_back(left_expr_tuple_0);
        right_exprs.emplace_back(right_expr_tuple_0);
      } else {
        left_exprs.emplace_back(right_expr_tuple_0);
        right_exprs.emplace_back(left_expr_tuple_0);
      }
    }
  }
}

}  // namespace bustub
