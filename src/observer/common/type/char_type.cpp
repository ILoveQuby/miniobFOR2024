/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"
#include "common/time/datetime.h"

int CharType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

int CharType::compare_like(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  auto match = [](const char *str, const char *pattern) -> int {
    const char *s     = str;
    const char *p     = pattern;
    const char *star  = nullptr;  // 记录上一个星号的位置
    const char *match = nullptr;  // 记录当前匹配的字符串位置
    while (*s) {
      // 如果当前字符匹配或者模式字符是 `_`
      if (*p == *s || *p == '_') {
        s++;
        p++;
      }
      // 如果模式字符是 `%`
      else if (*p == '%') {
        star  = p++;  // 记录 `%` 的位置
        match = s;    // 记录当前字符串位置
      }
      // 不匹配
      else if (star) {
        p = star + 1;  // 回到 `%` 的下一个字符
        s = ++match;   // 从下一个字符开始匹配
      }
      // 都不匹配
      else {
        return false;
      }
    }
    // 检查模式串是否处理完
    while (*p == '%') {
      p++;  // 跳过多余的 `%`
    }
    return *p == '\0';  // 模式串是否完全匹配
  };
  return match(left.value_.pointer_value_, right.value_.pointer_value_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::DATES: {
      result.attr_type_ = AttrType::DATES;
      int y, m, d;
      if (sscanf(val.value_.pointer_value_, "%d-%d-%d", &y, &m, &d) != 3) {
        LOG_WARN("invalid date format : %s", val.value_.pointer_value_);
        return RC::INVALID_ARGUMENT;
      }
      bool check_set = common::check_date(y, m, d);
      if (!check_set) {
        LOG_WARN("invalid date format : %s", val.value_.pointer_value_);
        return RC::INVALID_ARGUMENT;
      }
      result.set_date(y, m, d);
    } break;
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  if (type == AttrType::DATES) {
    return 1;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}