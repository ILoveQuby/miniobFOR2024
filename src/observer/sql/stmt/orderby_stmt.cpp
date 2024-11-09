#include "common/rc.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "sql/stmt/orderby_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/filter_stmt.h"

RC OrderByStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const std::vector<OrderBySqlNode> &orderby_sql_node, OrderByStmt *&stmt,
    std::vector<std::unique_ptr<Expression>> &&exprs)
{
  RC rc                     = RC::SUCCESS;
  stmt                      = nullptr;
  auto check_condition_expr = [&db, &tables, &default_table](Expression *expr) {
    if (expr->type() == ExprType::FIELD) {
      FieldExpr *field_expr = static_cast<FieldExpr *>(expr);
      return field_expr->check_field(*tables, {}, default_table, {});
    }
    return RC::SUCCESS;
  };
  std::vector<std::unique_ptr<OrderByUnit>> tmp_units;

  for (auto &node : orderby_sql_node) {
    RC rc = node.expr->traverse_check(check_condition_expr);
    if (OB_FAIL(rc))
      return RC::INVALID_ARGUMENT;
    tmp_units.emplace_back(
        std::make_unique<OrderByUnit>(node.expr, node.is_asc));  // 这里 order by unit 中的指针是独享的
  }
  stmt = new OrderByStmt();
  stmt->set_orderby_units(std::move(tmp_units));
  std::vector<std::unique_ptr<Expression>> expressions;
  for (auto &expr : exprs) {
    expressions.emplace_back(expr->deep_copy());
  }
  stmt->set_exprs(std::move(expressions));
  return rc;
}