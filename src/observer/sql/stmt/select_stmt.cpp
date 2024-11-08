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

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
  if (nullptr != having_stmt_) {
    delete having_stmt_;
    having_stmt_ = nullptr;
  }
}

RC SelectStmt::process_from_clause(Db *db, vector<Table *> &tables, unordered_map<string, string> &table_alias_map,
    unordered_map<string, Table *> &table_map, vector<InnerJoinSqlNode> &from_relations,
    vector<JoinTables> &join_tables, BinderContext &binder_context)
{
  unordered_set<string> table_alias_set;
  auto                  check_tables = [&](pair<string, string> table_name_pair) {
    string table_name = table_name_pair.first;
    string alias_name = table_name_pair.second;
    if (table_name.empty()) {
      LOG_WARN("invalid argument, relation name is null.");
      return RC::INVALID_ARGUMENT;
    }
    Table *table = db->find_table(table_name.c_str());
    if (table == nullptr) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    binder_context.add_table(table);
    binder_context.add_table_pair(table_name, table);
    tables.emplace_back(table);
    table_map.insert({table_name, table});
    if (!alias_name.empty()) {
      if (table_alias_set.count(alias_name) != 0) {
        return RC::INVALID_ARGUMENT;
      }
      binder_context.add_table_pair(alias_name, table);
      table_alias_set.insert(alias_name);
      table_alias_map.insert({table_name, alias_name});
      table_map.insert({alias_name, table});
    }
    return RC::SUCCESS;
  };
  auto process_one_relation = [&](const pair<string, string> &relation, JoinTables &jt, Expression *condition) {
    RC rc = RC::SUCCESS;
    rc    = check_tables(relation);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    FilterStmt *filter_stmt = nullptr;
    if (condition != nullptr) {
      rc = FilterStmt::create(db, table_map[relation.first], &table_map, condition, filter_stmt);
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }
    jt.push(table_map[relation.first], filter_stmt);
    return rc;
  };
  for (size_t i = 0; i < from_relations.size(); i++) {
    InnerJoinSqlNode &relations = from_relations[i];
    JoinTables        jt;
    RC                rc = process_one_relation(relations.base_relation, jt, nullptr);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    vector<pair<string, string>> &join_relations = relations.join_relations;
    vector<Expression *>         &conditions     = relations.conditions;
    for (size_t j = 0; j < join_relations.size(); j++) {
      rc = process_one_relation(join_relations[j], jt, conditions[j]);
      if (rc != RC::SUCCESS) {
        return rc;
      }
    }
    conditions.clear();
    join_tables.emplace_back(std::move(jt));
  }
  return RC::SUCCESS;
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt, unordered_map<string, Table *> parent_table_map)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument, db is null.");
    return RC::INVALID_ARGUMENT;
  }
  BinderContext                  binder_context;
  vector<Table *>                tables;
  unordered_map<string, string>  table_alias_map;
  unordered_map<string, Table *> table_map = parent_table_map;
  vector<JoinTables>             join_tables;

  RC rc =
      process_from_clause(db, tables, table_alias_map, table_map, select_sql.relations, join_tables, binder_context);
  if (rc != RC::SUCCESS) {
    return rc;
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
  FilterStmt                    *filter_stmt = nullptr;
  vector<unique_ptr<Expression>> filter_conditions;
  if (select_sql.conditions != nullptr) {
    unique_ptr<Expression> condition = select_sql.conditions->deep_copy();
    rc = FilterStmt::create(db, default_table, &table_map, select_sql.conditions, filter_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }

  FilterStmt                    *having_stmt = nullptr;
  vector<unique_ptr<Expression>> having_conditions;
  if (select_sql.having_conditions != nullptr) {
    unique_ptr<Expression> condition = select_sql.having_conditions->deep_copy();
    RC                     rc        = expression_binder.bind_expression(condition, having_conditions);
    rc = FilterStmt::create(db, default_table, &table_map, select_sql.having_conditions, having_stmt);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }

  SelectStmt *select_stmt = new SelectStmt();
  select_stmt->join_tables_.swap(join_tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->having_expressions_.swap(having_conditions);
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
