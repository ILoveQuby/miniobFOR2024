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
#include "sql/stmt/select_stmt.h"
#include "common/lang/defer.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

RC FilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    Expression *condition, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;
  if (condition == nullptr) {
    return rc;
  }
  auto check_condition_expr = [&db, &tables, &default_table](Expression *expr) {
    if (expr->type() == ExprType::FIELD) {
      FieldExpr *field_expr = static_cast<FieldExpr *>(expr);
      return field_expr->check_field(*tables, {}, default_table, {});
    }
    if (expr->type() == ExprType::SUBQUERY) {
      SubQueryExpr *sub_query_expr = static_cast<SubQueryExpr *>(expr);
      Stmt         *stmt           = nullptr;
      if (RC rc = SelectStmt::create(db, *sub_query_expr->get_sql_node(), stmt, *tables); rc != RC::SUCCESS)
        return rc;
      if (stmt->type() != StmtType::SELECT)
        return RC::INVALID_ARGUMENT;
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);
      if (select_stmt->query_expressions().size() > 1)
        return RC::INVALID_ARGUMENT;
      sub_query_expr->set_select_stmt(select_stmt);
      return RC::SUCCESS;
    }
    if (expr->type() == ExprType::COMPARISON) {
      ComparisonExpr *cmp_expr = static_cast<ComparisonExpr *>(expr);
      CompOp          comp     = cmp_expr->comp();
      if (comp < EQUAL_TO || comp >= NO_OP) {
        LOG_WARN("invalid compare operator : %d", comp);
        return RC::INVALID_ARGUMENT;
      }
      return RC::SUCCESS;
    }
    return RC::SUCCESS;
  };
  rc = condition->traverse_check(check_condition_expr);
  if (rc != RC::SUCCESS) {
    return rc;
  }
  FilterStmt *filter_stmt = new FilterStmt();
  filter_stmt->condition_ = std::unique_ptr<Expression>(condition);
  stmt                    = filter_stmt;
  return rc;
}