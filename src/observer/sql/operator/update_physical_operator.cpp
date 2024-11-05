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
// Created by WangYunlai on 2022/6/27.
//

#include "sql/operator/update_physical_operator.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];

  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_    = trx;
  int idx = 0;
  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple     *row_tuple = static_cast<RowTuple *>(tuple);
    Record       &record    = row_tuple->record();
    vector<Value> new_values(fields_.size());
    vector<Value> old_values(fields_.size());
    for (size_t i = 0; i < fields_.size(); i++) {
      Value     &values    = new_values[i];
      Value     &old_value = old_values[i];
      EmptyTuple tp;
      if (values_[i]->type() == ExprType::SUBQUERY) {
        SubQueryExpr *sub_query_expr = static_cast<SubQueryExpr *>(values_[i].get());
        rc                           = sub_query_expr->open(nullptr);
        if (rc != RC::SUCCESS)
          return rc;
        int val_count = 0;
        while (RC::SUCCESS == (rc = sub_query_expr->get_value(tp, values)))
          val_count++;
        if (val_count == 0)
          values.set_null();
        else if (val_count > 1)
          return RC::INVALID_ARGUMENT;
        sub_query_expr->close();
      } else {
        values_[i]->get_value(tp, values);
      }
      old_value.set_type(fields_[i].type());
      old_value.set_data(record.data() + fields_[i].offset(), fields_[i].len());
    }
    old_values_.emplace_back(std::move(old_values));
    rc = trx_->update_record(table_, record, new_values, fields_);
    if (rc != RC::SUCCESS) {
      for (int j = 0; j < idx; j++)
        table_->update_record(records_[j], old_values_[j], fields_);
      return rc;
    }
    records_.emplace_back(std::move(record));
    idx++;
  }

  child->close();
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }

RC UpdatePhysicalOperator::close() { return RC::SUCCESS; }
