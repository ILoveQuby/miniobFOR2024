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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "sql/expr/arithmetic_operator.hpp"
#include "sql/stmt/select_stmt.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "common/lang/defer.h"

using namespace std;

RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(table_name(), field_name()), value);
}

bool FieldExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::FIELD) {
    return false;
  }
  const auto &other_field_expr = static_cast<const FieldExpr &>(other);
  return get_table_name() == other_field_expr.get_table_name() && get_field_name() == other_field_expr.get_field_name();
}

// TODO: 在进行表达式计算时，`chunk` 包含了所有列，因此可以通过 `field_id` 获取到对应列。
// 后续可以优化成在 `FieldExpr` 中存储 `chunk` 中某列的位置信息。
RC FieldExpr::get_column(Chunk &chunk, Column &column)
{
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

bool ValueExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  const auto &other_value_expr = static_cast<const ValueExpr &>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return RC::SUCCESS;
}

RC ValueExpr::get_column(Chunk &chunk, Column &column)
{
  column.init(value_);
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }
  rc = Value::cast_to(value, cast_type_, cast_value);
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &result) const
{
  Value value;
  RC    rc = child_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

RC CastExpr::try_get_value(Value &result) const
{
  Value value;
  RC    rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  RC rc = RC::SUCCESS;
  if (comp_ == IS_NULL || comp_ == IS_NOT_NULL) {
    ASSERT(right.is_null(), "right is not null");
    result = comp_ == IS_NULL ? left.is_null() : !left.is_null();
    return rc;
  }
  if (left.is_null() || right.is_null()) {
    result = false;
    return rc;
  }
  if (comp_ == LIKE_OP || comp_ == NOT_LIKE_OP) {
    ASSERT(left.is_string() && right.is_string(), "left or right is not string");
    result = comp_ == LIKE_OP ? left.compare_like(right) : !left.compare_like(right);
    return rc;
  }
  int cmp_result = left.compare(right);
  result         = false;
  switch (comp_) {
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr   *left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr   *right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();
    const Value &right_cell       = right_value_expr->get_value();

    bool value = false;
    RC   rc    = compare_value(left_cell, right_cell, value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    } else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value         left_value;
  Value         right_value;
  SubQueryExpr *left_sub_query  = nullptr;
  SubQueryExpr *right_sub_query = nullptr;
  DEFER([&left_sub_query, &right_sub_query]() {
    if (left_sub_query != nullptr)
      left_sub_query->close();
    if (right_sub_query != nullptr)
      right_sub_query->close();
  });

  auto open_sub_query = [](const unique_ptr<Expression> &expr) {
    SubQueryExpr *sub = nullptr;
    if (expr->type() == ExprType::SUBQUERY) {
      sub = static_cast<SubQueryExpr *>(expr.get());
      sub->open(nullptr);
    }
    return sub;
  };
  left_sub_query  = open_sub_query(left_);
  right_sub_query = open_sub_query(right_);
  RC rc           = RC::SUCCESS;
  if (comp_ == EXISTS_OP || comp_ == NOT_EXISTS_OP) {
    rc = right_->get_value(tuple, right_value);
    value.set_boolean(comp_ == EXISTS_OP ? rc == RC::SUCCESS : rc == RC::RECORD_EOF);
    return rc == RC::RECORD_EOF ? RC::SUCCESS : rc;
  }
  if (left_sub_query) {
    int left_count = 0;
    while (RC::SUCCESS == (rc = left_->get_value(tuple, left_value)))
      left_count++;
    if (left_count > 1)
      return RC::INVALID_ARGUMENT;
  } else {
    rc = left_->get_value(tuple, left_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
      return rc;
    }
  }
  if (comp_ == IN_OP || comp_ == NOT_IN_OP) {
    if (left_value.is_null()) {
      value.set_boolean(false);
      return RC::SUCCESS;
    }
    if (right_->type() == ExprType::LIST) {
      static_cast<ListExpr *>(right_.get())->reset();
    }
    bool res = false, has_null = false;
    while (RC::SUCCESS == (rc = right_->get_value(tuple, right_value))) {
      if (right_value.is_null())
        has_null = true;
      else if (left_value.compare(right_value) == 0)
        res = true;
    }
    value.set_boolean(comp_ == IN_OP ? res : (has_null ? false : !res));
    return rc == RC::RECORD_EOF ? RC::SUCCESS : rc;
  }

  if (right_sub_query) {
    int right_count = 0;
    while (RC::SUCCESS == (rc = right_->get_value(tuple, right_value)))
      right_count++;
    if (right_count > 1) {
      return RC::INVALID_ARGUMENT;
    }
  } else {
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }
  bool bool_value = false;

  rc = compare_value(left_value, right_value, bool_value);
  if (rc == RC::SUCCESS) {
    value.set_boolean(bool_value);
  }
  return rc;
}

RC ComparisonExpr::eval(Chunk &chunk, std::vector<uint8_t> &select)
{
  RC     rc = RC::SUCCESS;
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");
    return RC::INTERNAL;
  }
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);
  } else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);
  } else {
    // TODO: support string compare
    LOG_WARN("unsupported data type %d", left_column.attr_type());
    return RC::INTERNAL;
  }
  return rc;
}

template <typename T>
RC ComparisonExpr::compare_column(const Column &left, const Column &right, std::vector<uint8_t> &result) const
{
  RC rc = RC::SUCCESS;

  bool left_const  = left.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    compare_result<T, true, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else if (left_const && !right_const) {
    compare_result<T, true, false>((T *)left.data(), (T *)right.data(), right.count(), result, comp_);
  } else if (!left_const && right_const) {
    compare_result<T, false, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else {
    compare_result<T, false, false>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> children)
    : conjunction_type_(type), children_(std::move(children))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == Type::AND);
  value.set_boolean(default_value);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

bool ArithmeticExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  auto &other_arith_expr = static_cast<const ArithmeticExpr &>(other);
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
         (right_ == nullptr || right_->equal(*other_arith_expr.right_));
}
AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  if (left_->value_type() == AttrType::NULLS || right_->value_type() == AttrType::NULLS) {
    return AttrType::NULLS;
  }

  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  if (target_type == AttrType::NULLS || left_value.is_null() || right_value.is_null()) {
    value.set_null();
    return rc;
  }
  value.set_type(target_type);

  switch (arithmetic_type_) {
    case Type::ADD: {
      Value::add(left_value, right_value, value);
    } break;

    case Type::SUB: {
      Value::subtract(left_value, right_value, value);
    } break;

    case Type::MUL: {
      Value::multiply(left_value, right_value, value);
    } break;

    case Type::DIV: {
      if (target_type == AttrType::INTS && right_value.get_int() == 0)
        value.set_null();
      else if (target_type == AttrType::FLOATS && right_value.get_float() > -1e-6 && right_value.get_float() < 1e-6)
        value.set_null();
      else
        Value::divide(left_value, right_value, value);
    } break;

    case Type::NEGATIVE: {
      Value::negative(left_value, value);
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
    const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;
  switch (type) {
    case Type::ADD: {
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
    } break;
    case Type::SUB:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::MUL:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::DIV:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::NEGATIVE:
      if (attr_type == AttrType::INTS) {
        unary_operator<LEFT_CONSTANT, int, NegateOperator>((int *)left.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        unary_operator<LEFT_CONSTANT, float, NegateOperator>(
            (float *)left.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    default: rc = RC::UNIMPLEMENTED; break;
  }
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  if (right_)
    rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
    return rc;
  }
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_column(left_column, right_column, column);
}

RC ArithmeticExpr::calc_column(const Column &left_column, const Column &right_column, Column &column) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  column.init(target_type, left_column.attr_len(), std::max(left_column.count(), right_column.count()));
  bool left_const  = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (left_const && !right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (!left_const && right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  return rc;
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

////////////////////////////////////////////////////////////////////////////////

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, Expression *child)
    : aggregate_name_(aggregate_name), child_(child)
{}

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, unique_ptr<Expression> child)
    : aggregate_name_(aggregate_name), child_(std::move(child))
{}

RC UnboundAggregateExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);
}

////////////////////////////////////////////////////////////////////////////////
AggregateExpr::AggregateExpr(Type type, Expression *child) : aggregate_type_(type), child_(child) {}

AggregateExpr::AggregateExpr(Type type, unique_ptr<Expression> child) : aggregate_type_(type), child_(std::move(child))
{}

RC AggregateExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    rc = RC::INTERNAL;
  }
  return rc;
}

bool AggregateExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != type()) {
    return false;
  }
  const AggregateExpr &other_aggr_expr = static_cast<const AggregateExpr &>(other);
  return aggregate_type_ == other_aggr_expr.aggregate_type() && child_->equal(*other_aggr_expr.child());
}

unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;
  switch (aggregate_type_) {
    case Type::SUM: {
      aggregator = make_unique<SumAggregator>();
      break;
    }
    case Type::AVG: {
      aggregator = make_unique<AvgAggregator>();
      break;
    }
    case Type::MAX: {
      aggregator = make_unique<MaxAggregator>();
      break;
    }
    case Type::MIN: {
      aggregator = make_unique<MinAggregator>();
      break;
    }
    case Type::COUNT: {
      if (child_->type() == ExprType::STAR)
        aggregator = make_unique<CountStarAggregator>();
      else
        aggregator = make_unique<CountAggregator>();
      break;
    }
    default: {
      ASSERT(false, "unsupported aggregate type");
      break;
    }
  }
  return aggregator;
}

RC AggregateExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);
}

RC AggregateExpr::type_from_string(const char *type_str, AggregateExpr::Type &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "count")) {
    type = Type::COUNT;
  } else if (0 == strcasecmp(type_str, "sum")) {
    type = Type::SUM;
  } else if (0 == strcasecmp(type_str, "avg")) {
    type = Type::AVG;
  } else if (0 == strcasecmp(type_str, "max")) {
    type = Type::MAX;
  } else if (0 == strcasecmp(type_str, "min")) {
    type = Type::MIN;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}

RC FieldExpr::check_field(const std::unordered_map<std::string, Table *> &table_map, const std::vector<Table *> &tables,
    Table *default_table, const std::unordered_map<std::string, std::string> &table_alias_map)
{
  ASSERT(field_name_ != "*", "ERROR!");
  const char *table_name = table_name_.c_str();
  const char *field_name = field_name_.c_str();
  Table      *table      = nullptr;
  if (!common::is_blank(table_name)) {  // 表名不为空
    // check table
    auto iter = table_map.find(table_name);
    if (iter == table_map.end()) {
      LOG_WARN("no such table in from list: %s", table_name);
      return RC::SCHEMA_FIELD_MISSING;
    }
    table = iter->second;
  } else {  // 表名为空，只有列名
    if (tables.size() != 1 && default_table == nullptr) {
      LOG_WARN("invalid. I do not know the attr's table. attr=%s", this->get_field_name().c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }
    table = default_table ? default_table : tables[0];
  }
  ASSERT(nullptr != table, "ERROR!");
  // set table_name
  table_name = table->name();
  // check field
  const FieldMeta *field_meta = table->table_meta().field(field_name);
  if (nullptr == field_meta) {
    LOG_WARN("no such field. field=%s.%s", table->name(), field_name);
    return RC::SCHEMA_FIELD_MISSING;
  }
  // set field_
  field_ = Field(table, field_meta);
  set_table_name(table_name);
  bool is_single_table = (tables.size() == 1);
  if (is_single_table) {
    set_name(field_name_);
  } else {
    set_name(table_name_ + "." + field_name_);
  }
  return RC::SUCCESS;
}

SubQueryExpr::SubQueryExpr(SelectSqlNode &sql_node)
{
  sql_node_ = make_unique<SelectSqlNode>();
  sql_node_->conditions.swap(sql_node.conditions);
  sql_node_->expressions.swap(sql_node.expressions);
  sql_node_->group_by.swap(sql_node.group_by);
  sql_node_->having_conditions.swap(sql_node.having_conditions);
  sql_node_->relations.swap(sql_node.relations);
}

SubQueryExpr::~SubQueryExpr() = default;

// 子算子树的 open 和 close 逻辑由外部控制
RC SubQueryExpr::open(Trx *trx) { return physical_oper_->open(trx); }

RC SubQueryExpr::close() { return physical_oper_->close(); }

bool SubQueryExpr::has_more_row(const Tuple &tuple) const
{
  // TODO(wbj) 这里没考虑其他错误
  physical_oper_->set_parent_tuple(&tuple);
  return physical_oper_->next() != RC::RECORD_EOF;
}

RC SubQueryExpr::get_value(const Tuple &tuple, Value &value) const
{
  physical_oper_->set_parent_tuple(&tuple);
  // 每次返回一行的第一个 cell
  if (RC rc = physical_oper_->next(); RC::SUCCESS != rc) {
    return rc;
  }
  return physical_oper_->current_tuple()->cell_at(0, value);
}

RC SubQueryExpr::try_get_value(Value &value) const { return RC::UNIMPLEMENTED; }

ExprType SubQueryExpr::type() const { return ExprType::SUBQUERY; }

AttrType SubQueryExpr::value_type() const { return AttrType::UNDEFINED; }

std::unique_ptr<Expression> SubQueryExpr::deep_copy() const { return {}; }

const std::unique_ptr<SelectSqlNode> &SubQueryExpr::get_sql_node() const { return sql_node_; }

void SubQueryExpr::set_select_stmt(SelectStmt *stmt) { stmt_ = std::unique_ptr<SelectStmt>(stmt); }

const std::unique_ptr<SelectStmt> &SubQueryExpr::get_select_stmt() const { return stmt_; }

void SubQueryExpr::set_logical_oper(std::unique_ptr<LogicalOperator> &&oper) { logical_oper_ = std::move(oper); }

const std::unique_ptr<LogicalOperator> &SubQueryExpr::get_logical_oper() { return logical_oper_; }

void SubQueryExpr::set_physical_oper(std::unique_ptr<PhysicalOperator> &&oper) { physical_oper_ = std::move(oper); }

const std::unique_ptr<PhysicalOperator> &SubQueryExpr::get_physical_oper() { return physical_oper_; }