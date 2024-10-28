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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument, db is null.");
    return RC::INVALID_ARGUMENT;
  }
  BinderContext                  binder_context;
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  unordered_map<string, Table *> local_table_map;
  vector<JoinTables>             join_tables;

  auto check_tables = [&](const char *table_name) {
    if (table_name == nullptr) {
      LOG_WARN("invalid argument, relation name is null.");
      return RC::INVALID_ARGUMENT;
    }
    Table *table = db->find_table(table_name);
    if (table == nullptr) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    binder_context.add_table(table);
    tables.push_back(table);
    table_map.insert({table_name, table});
    local_table_map.insert({table_name, table});
    return RC::SUCCESS;
  };

  std::vector<ConditionSqlNode> &all_conditions = select_sql.conditions;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const InnerJoinSqlNode                 &relations  = select_sql.relations[i];
    const vector<vector<ConditionSqlNode>> &conditions = relations.conditions;
    for (auto &on_conds : conditions) {
      all_conditions.insert(all_conditions.end(), on_conds.begin(), on_conds.end());
    }
  }

  auto check_can_push_down = [&local_table_map](const Expression *expr) {
    if (expr->type() == ExprType::AGGREGATION)
      return RC::INTERNAL;
    if (expr->type() == ExprType::FIELD) {
      const FieldExpr *field_expr = static_cast<const FieldExpr *>(expr);
      if (local_table_map.count(field_expr->get_table_name()) != 0)
        return RC::SUCCESS;
      return RC::INTERNAL;
    }
    return RC::SUCCESS;
  };
  auto cond_is_ok = [&check_can_push_down, &local_table_map](const ConditionSqlNode &node) -> bool {
    return RC::SUCCESS == node.left_expr->traverse_check(check_can_push_down) &&
           RC::SUCCESS == node.right_expr->traverse_check(check_can_push_down);
  };
  auto pick_conditions = [&cond_is_ok, &all_conditions]() {
    std::vector<ConditionSqlNode> res;
    for (auto iter = all_conditions.begin(); iter != all_conditions.end();) {
      if (cond_is_ok(*iter)) {
        res.emplace_back(*iter);
        iter = all_conditions.erase(iter);
      } else {
        iter++;
      }
    }
    return res;
  };
  auto process_one_relation = [&](const std::string &relation, JoinTables &jt) {
    RC rc = RC::SUCCESS;
    if (rc = check_tables(relation.c_str()); rc != RC::SUCCESS) {
      return rc;
    }
    auto        ok_conds    = pick_conditions();
    FilterStmt *filter_stmt = nullptr;
    if (!ok_conds.empty()) {
      if (rc = FilterStmt::create(db, table_map[relation], &table_map, ok_conds.data(), ok_conds.size(), filter_stmt);
          rc != RC::SUCCESS) {
        return rc;
      }
      ASSERT(nullptr != filter_stmt, "FilterStmt is null!");
    }

    // fill JoinTables
    jt.push(table_map[relation], filter_stmt);
    return rc;
  };

  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    InnerJoinSqlNode &relations = select_sql.relations[i];
    local_table_map.clear();
    JoinTables jt;
    RC         rc = process_one_relation(relations.base_relation, jt);
    if (rc != RC::SUCCESS)
      return rc;
    const std::vector<std::string> &join_relations = relations.join_relations;
    for (size_t j = 0; j < join_relations.size(); j++) {
      if (RC::SUCCESS != (rc = process_one_relation(join_relations[j], jt))) {
        return rc;
      }
    }
    join_tables.emplace_back(std::move(jt));
  }
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder               expression_binder(binder_context);
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }
  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }
  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }
  FilterStmt *filter_stmt = nullptr;
  if (!all_conditions.empty()) {
    RC rc = FilterStmt::create(
        db, default_table, &table_map, all_conditions.data(), static_cast<int>(all_conditions.size()), filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }

  FilterStmt *having_stmt = nullptr;
  if (!select_sql.having_conditions.empty()) {
    RC rc = FilterStmt::create(db,
        default_table,
        &table_map,
        select_sql.having_conditions.data(),
        static_cast<int>(select_sql.having_conditions.size()),
        having_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }
  vector<unique_ptr<Expression>> having_bound_expression;
  if (having_stmt != nullptr) {
    auto filter_units = having_stmt->filter_units();
    for (FilterUnit *unit : filter_units) {
      unique_ptr<Expression> left  = unit->left().expr->deep_copy();
      unique_ptr<Expression> right = unit->right().expr->deep_copy();
      if (left->type() != ExprType::VALUE)
        expression_binder.bind_expression(left, having_bound_expression);
      if (right->type() != ExprType::VALUE)
        expression_binder.bind_expression(right, having_bound_expression);
    }
  }
  SelectStmt *select_stmt = new SelectStmt();
  select_stmt->join_tables_.swap(join_tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->having_expressions_.swap(having_bound_expression);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->having_stmt_ = having_stmt;
  stmt                      = select_stmt;
  return RC::SUCCESS;
}
// {
//   if (nullptr == db) {
//     LOG_WARN("invalid argument. db is null");
//     return RC::INVALID_ARGUMENT;
//   }

//   BinderContext binder_context;

//   // collect tables in `from` statement
//   vector<Table *>                tables;
//   unordered_map<string, Table *> table_map;
//   for (size_t i = 0; i < select_sql.relations.size(); i++) {
//     const char *table_name = select_sql.relations[i].c_str();
//     if (nullptr == table_name) {
//       LOG_WARN("invalid argument. relation name is null. index=%d", i);
//       return RC::INVALID_ARGUMENT;
//     }

//     Table *table = db->find_table(table_name);
//     if (nullptr == table) {
//       LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
//       return RC::SCHEMA_TABLE_NOT_EXIST;
//     }

//     binder_context.add_table(table);
//     tables.push_back(table);
//     table_map.insert({table_name, table});
//   }

//   // collect query fields in `select` statement
//   vector<unique_ptr<Expression>> bound_expressions;
//   ExpressionBinder               expression_binder(binder_context);

//   for (unique_ptr<Expression> &expression : select_sql.expressions) {
//     RC rc = expression_binder.bind_expression(expression, bound_expressions);
//     if (OB_FAIL(rc)) {
//       LOG_INFO("bind expression failed. rc=%s", strrc(rc));
//       return rc;
//     }
//   }

//   vector<unique_ptr<Expression>> group_by_expressions;
//   for (unique_ptr<Expression> &expression : select_sql.group_by) {
//     RC rc = expression_binder.bind_expression(expression, group_by_expressions);
//     if (OB_FAIL(rc)) {
//       LOG_INFO("bind expression failed. rc=%s", strrc(rc));
//       return rc;
//     }
//   }

//   Table *default_table = nullptr;
//   if (tables.size() == 1) {
//     default_table = tables[0];
//   }

//   // create filter statement in `where` statement
//   FilterStmt *filter_stmt = nullptr;
//   RC          rc          = FilterStmt::create(db,
//       default_table,
//       &table_map,
//       select_sql.conditions.data(),
//       static_cast<int>(select_sql.conditions.size()),
//       filter_stmt);
//   if (rc != RC::SUCCESS) {
//     LOG_WARN("cannot construct filter stmt");
//     return rc;
//   }

//   // everything alright
//   SelectStmt *select_stmt = new SelectStmt();

//   select_stmt->tables_.swap(tables);
//   select_stmt->query_expressions_.swap(bound_expressions);
//   select_stmt->filter_stmt_ = filter_stmt;
//   select_stmt->group_by_.swap(group_by_expressions);
//   stmt = select_stmt;
//   return RC::SUCCESS;
// }
