#pragma once

#include "common/type/data_type.h"

class DateType : public DataType
{
public:
  DateType() : DataType(AttrType::DATES) {}
  virtual ~DateType() = default;

  int compare(const Value &left, const Value &right) const override;
  RC  min(const Value &left, const Value &right, Value &result) const override;
  RC  max(const Value &left, const Value &right, Value &result) const override;

  RC to_string(const Value &val, string &result) const override;
};