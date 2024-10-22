/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/filter_stmt.h"
#include "common/lang/defer.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

FilterStmt::~FilterStmt()
{
  for (FilterUnit *unit : filter_units_) {
    delete unit;
  }
  filter_units_.clear();
}

RC FilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode *conditions, int condition_num, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;

  FilterStmt *tmp_stmt = new FilterStmt();
  for (int i = 0; i < condition_num; i++) {
    FilterUnit *filter_unit = nullptr;

    rc = create_filter_unit(db, default_table, tables, conditions[i], filter_unit);
    if (rc != RC::SUCCESS) {
      delete tmp_stmt;
      LOG_WARN("failed to create filter unit. condition index=%d", i);
      return rc;
    }
    tmp_stmt->filter_units_.push_back(filter_unit);
  }

  stmt = tmp_stmt;
  return rc;
}

RC FilterStmt::create_filter_unit(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const ConditionSqlNode &condition, FilterUnit *&filter_unit)
{
  RC rc = RC::SUCCESS;

  CompOp comp = condition.comp;
  if (comp < EQUAL_TO || comp >= NO_OP) {
    LOG_WARN("invalid compare operator : %d", comp);
    return RC::INVALID_ARGUMENT;
  }

  filter_unit = new FilterUnit;
  DEFER([&]() {
    if (RC::SUCCESS != rc && nullptr != filter_unit) {
      delete filter_unit;
      filter_unit = nullptr;
    }
  });

  Expression                *left = nullptr;
  const std::vector<Table *> table_arr;
  rc = condition.left_expr->create_expression(*tables, table_arr, db, left, default_table);
  if (rc != RC::SUCCESS) {
    LOG_WARN("filter_stmt create lhs expression error");
    return rc;
  }
  Expression *right = nullptr;
  rc                = condition.right_expr->create_expression(*tables, table_arr, db, right, default_table);
  if (rc != RC::SUCCESS) {
    LOG_WARN("filter_stmt create rhs expression error");
    return rc;
  }
  ASSERT(left!= nullptr,"filter_stmt create lhs expression error");
  ASSERT(right!= nullptr,"filter_stmt create rhs expression error");
  FilterObj left_filter_obj, right_filter_obj;
  left_filter_obj.expr  = left;
  right_filter_obj.expr = right;
  delete condition.left_expr;
  delete condition.right_expr;
  filter_unit->set_left(left_filter_obj);
  filter_unit->set_right(right_filter_obj);
  filter_unit->set_comp(comp);
  // 检查两个类型是否能够比较
  return rc;
}
