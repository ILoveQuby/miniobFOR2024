#include "sql/operator/orderby_logical_operator.h"

OrderByLogicalOperator::OrderByLogicalOperator(
    std::vector<std::unique_ptr<OrderByUnit>> &&orderby_units, std::vector<std::unique_ptr<Expression>> &&exprs)
    : orderby_units_(std::move(orderby_units)), exprs_(std::move(exprs))
{}
