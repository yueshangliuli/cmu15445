//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_plan.h
//
// Identification: src/include/execution/plans/index_scan_plan.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <utility>

#include <vector>
#include "catalog/catalog.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/abstract_plan.h"

namespace bustub {
/**
 * IndexScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
class IndexScanPlanNode : public AbstractPlanNode {
 public:
  /**
   * Creates a new index scan plan node.
   * @param output the output format of this scan plan node
   * @param table_oid the identifier of table to be scanned
   */
  IndexScanPlanNode(SchemaRef output, index_oid_t index_oid)
      : AbstractPlanNode(std::move(output), {}), index_oid_(index_oid) {}

  IndexScanPlanNode(SchemaRef output, index_oid_t index_oid, std::vector<Value> key_values,
                    AbstractExpressionRef predicate, bool single_search = false)
      : AbstractPlanNode(std::move(output), {}),
        index_oid_(index_oid),
        key_values_(std::move(key_values)),
        predicate_(std::move(predicate)),
        single_search_(single_search) {}

  auto GetType() const -> PlanType override { return PlanType::IndexScan; }

  /** @return the identifier of the table that should be scanned */
  auto GetIndexOid() const -> index_oid_t { return index_oid_; }

  auto GetKeyValues() const -> std::vector<Value> { return key_values_; };

  auto Predicate() const -> const AbstractExpressionRef & { return predicate_; }

  BUSTUB_PLAN_NODE_CLONE_WITH_CHILDREN(IndexScanPlanNode);

  /** The table whose tuples should be scanned. */
  index_oid_t index_oid_;
  std::vector<Value> key_values_;
  AbstractExpressionRef predicate_;
  bool single_search_;

  // Add anything you want here for index lookup

 protected:
  auto PlanNodeToString() const -> std::string override {
    return fmt::format("IndexScan {{ index_oid={} }}", index_oid_);
  }
};

}  // namespace bustub
