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

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/select_stmt.h"

UpdateStmt::UpdateStmt(Table *table, std::vector<FieldMeta> fields, std::vector<std::unique_ptr<Expression>> &&values,
    FilterStmt *filter_stmt)
    : table_(table), values_(std::move(values)), fields_(fields), filter_stmt_(filter_stmt)
{}

UpdateStmt::~UpdateStmt()
{
  if (filter_stmt_ != nullptr) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  const char *table_name = update.relation_name.c_str();
  if (db == nullptr || table_name == nullptr) {
    LOG_WARN("invalid argument. db=%p, table_name=%p",db, table_name);
    return RC::INVALID_ARGUMENT;
  }
  Table *table = db->find_table(table_name);
  if (table == nullptr) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  std::vector<std::unique_ptr<Expression>> values;
  std::vector<FieldMeta>                   fields;
  const TableMeta                         &table_meta = table->table_meta();
  for (size_t i = 0; i < update.attribute_names.size(); i++) {
    const FieldMeta *update_field = table_meta.field(update.attribute_names[i].c_str());
    if (update_field == nullptr)
      return RC::INVALID_ARGUMENT;
    auto check_field = [&](Expression *expr) {
      if (expr->type() == ExprType::SUBQUERY) {
        SubQueryExpr *sub_query_expr = static_cast<SubQueryExpr *>(expr);
        Stmt         *stmt           = nullptr;
        if (RC rc = SelectStmt::create(db, *sub_query_expr->get_sql_node(), stmt, {}); rc != RC::SUCCESS)
          return rc;
        if (stmt->type() != StmtType::SELECT)
          return RC::INVALID_ARGUMENT;
        SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);
        if (select_stmt->query_expressions().size() > 1)
          return RC::INVALID_ARGUMENT;
        sub_query_expr->set_select_stmt(select_stmt);
        return RC::SUCCESS;
      }
      return RC::SUCCESS;
    };

    if (update.values[i]->type() == ExprType::VALUE) {
      const Value &val = static_cast<ValueExpr *>(update.values[i])->get_value();
      if (val.is_null()) {
        if (!update_field->nullable())
          return RC::INVALID_ARGUMENT;
      } else {
        if (val.attr_type() != update_field->type())
          return RC::INVALID_ARGUMENT;
      }
    } else {
      RC rc = update.values[i]->traverse_check(check_field);
      if (rc != RC::SUCCESS)
        return rc;
    }
    fields.emplace_back(*update_field);
    values.emplace_back(update.values[i]);
  }

  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));

  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db, table, &table_map, update.conditions, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }
  stmt = new UpdateStmt(table, std::move(fields), std::move(values), filter_stmt);
  return RC::SUCCESS;
}
