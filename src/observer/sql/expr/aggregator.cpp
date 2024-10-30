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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  if (!value.is_null())
    Value::add(value, value_, value_);
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value &result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  if (!value.is_null())
    Value::max(value, value_, value_);
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value &result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  if (!value.is_null())
    Value::min(value, value_, value_);
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value &result)
{
  result = value_;
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  if (!value.is_null()) {
    Value::add(value, value_, value_);
    cnt_++;
  }
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value &result)
{
  float val = value_.get_float();
  if (cnt_ > 0)
    result = Value(val / cnt_);
  else
    result.set_null();
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (!value.is_null())
    cnt_++;
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value &result)
{
  result = Value(cnt_);
  return RC::SUCCESS;
}

RC CountStarAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  cnt_++;
  return RC::SUCCESS;
}

RC CountStarAggregator::evaluate(Value &result)
{
  result = Value(cnt_);
  return RC::SUCCESS;
}