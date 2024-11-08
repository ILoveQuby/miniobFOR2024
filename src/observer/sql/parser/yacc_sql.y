
%{

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

#include "common/log/log.h"
#include "common/lang/string.h"
#include "common/time/datetime.h"
#include "sql/parser/parse_defs.h"
#include "sql/parser/yacc_sql.hpp"
#include "sql/parser/lex_sql.h"
#include "sql/expr/expression.h"

using namespace std;

string token_name(const char *sql_string, YYLTYPE *llocp)
{
  return string(sql_string + llocp->first_column, llocp->last_column - llocp->first_column + 1);
}

int yyerror(YYLTYPE *llocp, const char *sql_string, ParsedSqlResult *sql_result, yyscan_t scanner, const char *msg, bool is_date = false)
{
  std::unique_ptr<ParsedSqlNode> error_sql_node = std::make_unique<ParsedSqlNode>(SCF_ERROR);
  error_sql_node->error.error_msg = msg;
  error_sql_node->error.line = llocp->first_line;
  error_sql_node->error.column = llocp->first_column;
  error_sql_node->error.is_date = is_date;
  sql_result->add_sql_node(std::move(error_sql_node));
  return 0;
}

ArithmeticExpr *create_arithmetic_expression(ArithmeticExpr::Type type,
                                             Expression *left,
                                             Expression *right,
                                             const char *sql_string,
                                             YYLTYPE *llocp)
{
  ArithmeticExpr *expr = new ArithmeticExpr(type, left, right);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

UnboundAggregateExpr *create_aggregate_expression(const char *aggregate_name,
                                           Expression *child,
                                           const char *sql_string,
                                           YYLTYPE *llocp)
{
  UnboundAggregateExpr *expr = new UnboundAggregateExpr(aggregate_name, child);
  expr->set_name(token_name(sql_string, llocp));
  return expr;
}

%}

%define api.pure full
%define parse.error verbose
/** 启用位置标识 **/
%locations
%lex-param { yyscan_t scanner }
/** 这些定义了在yyparse函数中的参数 **/
%parse-param { const char * sql_string }
%parse-param { ParsedSqlResult * sql_result }
%parse-param { void * scanner }

//标识tokens
%token  SEMICOLON
        BY
        CREATE
        DROP
        GROUP
        TABLE
        TABLES
        INDEX
        CALC
        SELECT
        DESC
        SHOW
        SYNC
        INSERT
        DELETE
        UPDATE
        LBRACE
        RBRACE
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
        STRING_T
        DATE_T
        FLOAT_T
        HELP
        EXIT
        DOT //QUOTE
        INTO
        VALUES
        FROM
        WHERE
        AND
        OR
        SET
        ON
        LOAD
        DATA
        INFILE
        EXPLAIN
        STORAGE
        FORMAT
        AS
        EQ
        LT
        GT
        LE
        GE
        NE
        LIKE
        IS
        IN
        NOT
        NULL_T
        INNER
        JOIN
        HAVING
        EXISTS
        UNIQUE


/** union 中定义各种数据类型，真实生成的代码也是union类型，所以不能有非POD类型的数据 **/
%union {
  ParsedSqlNode *                            sql_node;
  Value *                                    value;
  enum CompOp                                comp;
  RelAttrSqlNode *                           rel_attr;
  std::vector<AttrInfoSqlNode> *             attr_infos;
  AttrInfoSqlNode *                          attr_info;
  Expression *                               expression;
  std::vector<std::unique_ptr<Expression>> * expression_list;
  UpdateKv *                                 update_kv;
  std::vector<UpdateKv> *                    update_kv_list;
  std::vector<Value> *                       value_list;
  std::vector<std::vector<Value>> *          insert_value_list;
  std::vector<RelAttrSqlNode> *              rel_attr_list;
  std::vector<std::string> *                 relation_list;
  InnerJoinSqlNode *                         inner_joins;
  std::vector<InnerJoinSqlNode> *            inner_joins_list;
  char *                                     string;
  int                                        number;
  float                                      floats;
  bool                                       boolean;
}

%token <number> NUMBER
%token <floats> FLOAT
%token <string> MAX
%token <string> MIN
%token <string> SUM
%token <string> AVG
%token <string> COUNT
%token <string> ID
%token <string> SSS
%token <string> DATE_STR
//非终结符

/** type 定义了各种解析后的结果输出的是什么类型。类型对应了 union 中的定义的成员变量名称 **/
%type <number>              type
%type <expression>          condition
%type <value>               value
%type <number>              number
%type <comp>                comp_op
%type <comp>                exists_op
%type <rel_attr>            rel_attr
%type <attr_infos>          attr_def_list
%type <attr_info>           attr_def
%type <value_list>          value_list
%type <expression>          where
%type <string>              alias 
%type <string>              storage_format
%type <string>              aggregate_type
%type <boolean>             unique_op
%type <expression>          sub_query_expr
%type <expression>          expression
%type <expression_list>     expression_list
%type <value_list>          insert_value
%type <insert_value_list>   insert_value_list
%type <relation_list>       id_list
%type <update_kv_list>      update_kv_list
%type <update_kv>           update_kv
%type<inner_joins>          join_list
%type<inner_joins>          from_node
%type<inner_joins_list>     from_list
%type <expression_list>     group_by
%type <expression>          opt_having
%type <sql_node>            calc_stmt
%type <sql_node>            select_stmt
%type <sql_node>            insert_stmt
%type <sql_node>            update_stmt
%type <sql_node>            delete_stmt
%type <sql_node>            create_table_stmt
%type <sql_node>            drop_table_stmt
%type <sql_node>            show_tables_stmt
%type <sql_node>            desc_table_stmt
%type <sql_node>            create_index_stmt
%type <sql_node>            drop_index_stmt
%type <sql_node>            sync_stmt
%type <sql_node>            begin_stmt
%type <sql_node>            commit_stmt
%type <sql_node>            rollback_stmt
%type <sql_node>            load_data_stmt
%type <sql_node>            explain_stmt
%type <sql_node>            set_variable_stmt
%type <sql_node>            help_stmt
%type <sql_node>            exit_stmt
%type <sql_node>            command_wrapper
// commands should be a list but I use a single command instead
%type <sql_node>            commands

%left '+' '-'
%left '*' '/'
%nonassoc UMINUS
%%

commands: command_wrapper opt_semicolon  //commands or sqls. parser starts here.
  {
    std::unique_ptr<ParsedSqlNode> sql_node = std::unique_ptr<ParsedSqlNode>($1);
    sql_result->add_sql_node(std::move(sql_node));
  }
  ;

command_wrapper:
    calc_stmt
  | select_stmt
  | insert_stmt
  | update_stmt
  | delete_stmt
  | create_table_stmt
  | drop_table_stmt
  | show_tables_stmt
  | desc_table_stmt
  | create_index_stmt
  | drop_index_stmt
  | sync_stmt
  | begin_stmt
  | commit_stmt
  | rollback_stmt
  | load_data_stmt
  | explain_stmt
  | set_variable_stmt
  | help_stmt
  | exit_stmt
    ;

exit_stmt:      
    EXIT {
      (void)yynerrs;  // 这么写为了消除yynerrs未使用的告警。如果你有更好的方法欢迎提PR
      $$ = new ParsedSqlNode(SCF_EXIT);
    };

help_stmt:
    HELP {
      $$ = new ParsedSqlNode(SCF_HELP);
    };

sync_stmt:
    SYNC {
      $$ = new ParsedSqlNode(SCF_SYNC);
    }
    ;

begin_stmt:
    TRX_BEGIN  {
      $$ = new ParsedSqlNode(SCF_BEGIN);
    }
    ;

commit_stmt:
    TRX_COMMIT {
      $$ = new ParsedSqlNode(SCF_COMMIT);
    }
    ;

rollback_stmt:
    TRX_ROLLBACK  {
      $$ = new ParsedSqlNode(SCF_ROLLBACK);
    }
    ;

drop_table_stmt:    /*drop table 语句的语法解析树*/
    DROP TABLE ID {
      $$ = new ParsedSqlNode(SCF_DROP_TABLE);
      $$->drop_table.relation_name = $3;
      free($3);
    };

show_tables_stmt:
    SHOW TABLES {
      $$ = new ParsedSqlNode(SCF_SHOW_TABLES);
    }
    ;

desc_table_stmt:
    DESC ID  {
      $$ = new ParsedSqlNode(SCF_DESC_TABLE);
      $$->desc_table.relation_name = $2;
      free($2);
    }
    ;

create_index_stmt:    /*create index 语句的语法解析树*/
    CREATE unique_op INDEX ID ON ID LBRACE ID id_list RBRACE
    {
      $$ = new ParsedSqlNode(SCF_CREATE_INDEX);
      CreateIndexSqlNode &create_index = $$->create_index;
      create_index.unique = $2;
      create_index.index_name = $4;
      create_index.relation_name = $6;
      if($9 != nullptr) {
        create_index.attribute_names.swap(*$9);
        delete $9;
      }
      create_index.attribute_names.emplace_back($8);
      std::reverse(create_index.attribute_names.begin(), create_index.attribute_names.end());
      free($4);
      free($6);
      free($8);
    }
    ;
unique_op:
    /* empty */ 
    {
      $$ = false;
    }
    | UNIQUE
    {
      $$ = true;
    }
    ;
id_list:
    /* empty */ 
    {
      $$ = nullptr;
    }
    | COMMA ID id_list
    {
      if($3 != nullptr)
      {
        $$ = $3;
      }
      else {
        $$ = new std::vector<std::string>;
      }
      $$->emplace_back($2);
      free($2);
    }
    ;

drop_index_stmt:      /*drop index 语句的语法解析树*/
    DROP INDEX ID ON ID
    {
      $$ = new ParsedSqlNode(SCF_DROP_INDEX);
      $$->drop_index.index_name = $3;
      $$->drop_index.relation_name = $5;
      free($3);
      free($5);
    }
    ;
create_table_stmt:    /*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def attr_def_list RBRACE storage_format
    {
      $$ = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = $$->create_table;
      create_table.relation_name = $3;
      free($3);

      std::vector<AttrInfoSqlNode> *src_attrs = $6;

      if (src_attrs != nullptr) {
        create_table.attr_infos.swap(*src_attrs);
        delete src_attrs;
      }
      create_table.attr_infos.emplace_back(*$5);
      std::reverse(create_table.attr_infos.begin(), create_table.attr_infos.end());
      delete $5;
      if ($8 != nullptr) {
        create_table.storage_format = $8;
        free($8);
      }
    }
    ;
attr_def_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA attr_def attr_def_list
    {
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<AttrInfoSqlNode>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;
    
attr_def:
    ID type LBRACE number RBRACE 
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = $4;
      $$->nullable = false;
      free($1);
    }
    | ID type LBRACE number RBRACE NOT NULL_T
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = $4;
      $$->nullable = false;
      free($1);
    }
    | ID type LBRACE number RBRACE NULL_T
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = $4;
      $$->nullable = true;
      free($1);
    }
    | ID type
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = 4;
      $$->nullable = false;
      free($1);
    }
    | ID type NOT NULL_T
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = 4;
      $$->nullable = false;
      free($1);
    }
    | ID type NULL_T
    {
      $$ = new AttrInfoSqlNode;
      $$->type = (AttrType)$2;
      $$->name = $1;
      $$->length = 4;
      $$->nullable = true;
      free($1);
    }
    ;
number:
    NUMBER {$$ = $1;}
    ;
type:
    INT_T      { $$ = static_cast<int>(AttrType::INTS); }
    | STRING_T { $$ = static_cast<int>(AttrType::CHARS); }
    | FLOAT_T  { $$ = static_cast<int>(AttrType::FLOATS); }
    | DATE_T   {$$ = static_cast<int>(AttrType::DATES); }
    ;
insert_stmt:        /*insert   语句的语法解析树*/
    INSERT INTO ID VALUES insert_value insert_value_list 
    {
      $$ = new ParsedSqlNode(SCF_INSERT);
      $$->insertion.relation_name = $3;
      if($6 != nullptr) {
        $$->insertion.values.swap(*$6);
      }
      $$->insertion.values.emplace_back(*$5);
      std::reverse($$->insertion.values.begin(), $$->insertion.values.end());
      delete $5;
      free($3);
    }
    ;
insert_value_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA insert_value insert_value_list
    {
      if($3 != nullptr) {
        $$ = $3;
      }
      else {
        $$ = new std::vector<std::vector<Value>>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;
insert_value:
    LBRACE value value_list RBRACE
    {
      if($3 != nullptr) {
        $$ = $3;
      }
      else {
        $$ = new std::vector<Value>;
      }
      $$->emplace_back(*$2);
      std::reverse($$->begin(), $$->end());
      delete $2;
    }
    ;

value_list:
    /* empty */
    {
      $$ = nullptr;
    }
    | COMMA value value_list  { 
      if ($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<Value>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;
value:
    NUMBER {
      $$ = new Value((int)$1);
      @$ = @1;
    }
    |FLOAT {
      $$ = new Value((float)$1);
      @$ = @1;
    }
    | DATE_STR {
      char *tmp = common::substr($1,1,strlen($1)-2);
      std::string str(tmp);
      Value *value = new Value();
      int date;
      if(common::string_to_date(str, date) < 0)
      {
        free(tmp);
        free($1);
        yyerror(&@$, sql_string, sql_result, scanner, "error", true);
        YYERROR;
      }
      value->set_date(date);
      $$ = value;
      free(tmp);
      free($1);
    }
    |NULL_T {
      $$ = new Value();
      $$->set_null();
    }
    |SSS {
      char *tmp = common::substr($1,1,strlen($1)-2);
      $$ = new Value(tmp);
      free(tmp);
      free($1);
    }
    ;
storage_format:
    /* empty */
    {
      $$ = nullptr;
    }
    | STORAGE FORMAT EQ ID
    {
      $$ = $4;
    }
    ;
    
delete_stmt:    /*  delete 语句的语法解析树*/
    DELETE FROM ID where 
    {
      $$ = new ParsedSqlNode(SCF_DELETE);
      $$->deletion.relation_name = $3;
      if ($4 != nullptr) {
        $$->deletion.conditions = $4;
      }
      free($3);
    }
    ;
update_stmt:      /*  update 语句的语法解析树*/
    UPDATE ID SET update_kv update_kv_list where 
    {
      $$ = new ParsedSqlNode(SCF_UPDATE);
      $$->update.relation_name = $2;
      $$->update.attribute_names.emplace_back($4->attribute_name);
      $$->update.values.emplace_back($4->value);
      if($5 != nullptr) {
        for(UpdateKv kv : *$5) {
          $$->update.attribute_names.emplace_back(kv.attribute_name);
          $$->update.values.emplace_back(kv.value);
        }
        delete $5;
      }
      if($6 != nullptr) {
        $$->update.conditions = $6;
      }
      free($2);
      delete $4;
    }
    ;

update_kv_list:
    /* empty */ 
    {
      $$ = nullptr;
    }
    | COMMA update_kv update_kv_list 
    {
      if($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<UpdateKv>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;

update_kv:
    ID EQ expression
    {
      $$ = new UpdateKv;
      $$->attribute_name = $1;
      $$->value = $3;
      free($1);
    }
    ;

alias:
    /* empty */ {
      $$ = nullptr;
    }
    | ID {
      $$ = $1;
    }
    | AS ID {
      $$ = $2;
    }
    ;

from_list:
    /* empty */ {
      $$ = nullptr;
    }
    | COMMA from_node from_list {
      if($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new std::vector<InnerJoinSqlNode>;
      }
      $$->emplace_back(*$2);
      delete $2;
    }
    ;

from_node:
    ID alias join_list {
      if($3 != nullptr) {
        $$ = $3;
      } else {
        $$ = new InnerJoinSqlNode;
      }
      $$->base_relation.first = $1;
      $$->base_relation.second = $2 == nullptr ? "" : std::string($2);
      std::reverse($$->join_relations.begin(), $$->join_relations.end());
      std::reverse($$->conditions.begin(), $$->conditions.end());
      free($1);
      free($2);
    }
    ;

join_list:
    /* empty */ {
      $$ = nullptr;
    }
    | INNER JOIN ID alias ON condition join_list {
      if($7 != nullptr) {
        $$ = $7;
      } else {
        $$ = new InnerJoinSqlNode;
      }
      std::string tmp = "";
      if($4 != nullptr) {
        tmp = $4;
      }
      $$->join_relations.emplace_back($3, tmp);
      $$->conditions.emplace_back($6);
      free($3);
      free($4);
    }
    ;

select_stmt:        /*  select 语句的语法解析树*/
    SELECT expression_list FROM from_node from_list where group_by opt_having
    {
      $$ = new ParsedSqlNode(SCF_SELECT);
      if ($2 != nullptr) {
        $$->selection.expressions.swap(*$2);
        delete $2;
      }

      if($5 != nullptr) {
        $$->selection.relations.swap(*$5);
        delete $5;
      }
      $$->selection.relations.emplace_back(*$4);
      std::reverse($$->selection.relations.begin(), $$->selection.relations.end());

      if ($6 != nullptr) {
        $$->selection.conditions = $6;
      }

      if ($7 != nullptr) {
        $$->selection.group_by.swap(*$7);
        delete $7;
      }

      if($8 != nullptr) {
        $$->selection.having_conditions = $8;
      }

      delete $4;
    }
    ;
calc_stmt:
    CALC expression_list
    {
      $$ = new ParsedSqlNode(SCF_CALC);
      $$->calc.expressions.swap(*$2);
      delete $2;
    }
    ;

expression_list:
    expression alias
    {
      $$ = new std::vector<std::unique_ptr<Expression>>;
      if($2 != nullptr) {
        $1->set_alias($2);
      }
      $$->emplace_back($1);
      free($2);
    }
    | expression alias COMMA expression_list
    {
      if ($4 != nullptr) {
        $$ = $4;
      } else {
        $$ = new std::vector<std::unique_ptr<Expression>>;
      }
      if($2 != nullptr) {
        $1->set_alias($2);
      }
      $$->emplace($$->begin(), $1);
      free($2);
    }
    ;
expression:
    expression '+' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::ADD, $1, $3, sql_string, &@$);
    }
    | expression '-' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::SUB, $1, $3, sql_string, &@$);
    }
    | expression '*' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::MUL, $1, $3, sql_string, &@$);
    }
    | expression '/' expression {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::DIV, $1, $3, sql_string, &@$);
    }
    | LBRACE expression RBRACE {
      $$ = $2;
      $$->set_name(token_name(sql_string, &@$));
    }
    | LBRACE expression COMMA expression_list RBRACE {
      $$ = new ListExpr($2, std::move(*$4));
      $$->set_name(token_name(sql_string, &@$));
      delete $4;
    }
    | '-' expression %prec UMINUS {
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::NEGATIVE, $2, nullptr, sql_string, &@$);
    }
    | value {
      $$ = new ValueExpr(*$1);
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
    | rel_attr {
      FieldExpr *tmp = new FieldExpr();
      tmp->set_table_name($1->relation_name);
      tmp->set_field_name($1->attribute_name);
      $$ = tmp;
      $$->set_name(token_name(sql_string, &@$));
      delete $1;
    }
    | '*' DOT '*' {
      $$ = new StarExpr("*");
    }
    | ID DOT '*' {
      $$ = new StarExpr($1);
      free($1);
    }
    | '*' {
      $$ = new StarExpr();
    }
    | expression value {
      if(!$2->is_minus())
      {
        yyerror(&@$, sql_string, sql_result, scanner, "error", false);
        YYERROR;
      }
      ValueExpr *val = new ValueExpr(*$2);
      $$ = create_arithmetic_expression(ArithmeticExpr::Type::ADD, $1, val, sql_string, &@$);
      delete $2;
    }
    | aggregate_type LBRACE expression RBRACE {
      $$ = create_aggregate_expression($1, $3, sql_string, &@$);
      free($1);
    }
    | aggregate_type LBRACE expression COMMA expression_list RBRACE {
      $$ = create_aggregate_expression("MAX", new StarExpr(), sql_string, &@$);
      free($1);
      delete $3;
      delete $5;
    }
    | aggregate_type LBRACE RBRACE {
      $$ = create_aggregate_expression("MAX", new StarExpr(), sql_string, &@$);
      free($1);
    }
    | sub_query_expr {
      $$ = $1;
    }
    // your code here
    ;

sub_query_expr:
    LBRACE select_stmt RBRACE
    {
      $$ = new SubQueryExpr($2->selection);
      delete $2;
    }
    ;

aggregate_type:
    SUM {
      $$ = $1;
    }
    | MAX {
      $$ = $1;
    }
    | MIN {
      $$ = $1;
    }
    | AVG {
      $$ = $1;
    }
    | COUNT {
      $$ = $1;
    }
    ;

rel_attr:
    ID {
      $$ = new RelAttrSqlNode;
      $$->attribute_name = $1;
      free($1);
    }
    | ID DOT ID {
      $$ = new RelAttrSqlNode;
      $$->relation_name  = $1;
      $$->attribute_name = $3;
      free($1);
      free($3);
    }
    ;

where:
    /* empty */
    {
      $$ = nullptr;
    }
    | WHERE condition {
      $$ = $2;  
    }
    ;

condition:
    expression comp_op expression
    {
      $$ = new ComparisonExpr($2, $1, $3);
    }
    | expression IS NULL_T
    {
      Value val;
      val.set_null();
      ValueExpr *value_expr = new ValueExpr(val);
      $$ = new ComparisonExpr(IS_NULL, $1, value_expr);
    }
    | expression IS NOT NULL_T
    {
      Value val;
      val.set_null();
      ValueExpr *value_expr = new ValueExpr(val);
      $$ = new ComparisonExpr(IS_NOT_NULL, $1, value_expr);
    }
    | exists_op expression 
    {
      Value val;
      val.set_null();
      ValueExpr *value_expr = new ValueExpr(val);
      $$ = new ComparisonExpr($1, value_expr, $2);
    }
    | condition AND condition
    {
      $$ = new ConjunctionExpr(ConjunctionExpr::Type::AND, $1, $3);
    }
    | condition OR condition
    {
      $$ = new ConjunctionExpr(ConjunctionExpr::Type::OR, $1, $3);
    }
    ;

comp_op:
      EQ { $$ = EQUAL_TO; }
    | LT { $$ = LESS_THAN; }
    | GT { $$ = GREAT_THAN; }
    | LE { $$ = LESS_EQUAL; }
    | GE { $$ = GREAT_EQUAL; }
    | NE { $$ = NOT_EQUAL; }
    | LIKE { $$ = LIKE_OP; }
    | NOT LIKE { $$ = NOT_LIKE_OP; }
    | IN { $$ = IN_OP; }
    | NOT IN { $$ = NOT_IN_OP; }
    ;

exists_op:
    EXISTS { $$ = EXISTS_OP; }
    | NOT EXISTS { $$ = NOT_EXISTS_OP; }
    ;

// your code here
group_by:
    /* empty */
    {
      $$ = nullptr;
    }
    | GROUP BY expression_list
    {
      $$ = $3;
    }
    ;

opt_having:
    /* empty */ {
      $$ = nullptr;
    }
    | HAVING condition 
    {
      $$ = $2;
    }
    ;

load_data_stmt:
    LOAD DATA INFILE SSS INTO TABLE ID 
    {
      char *tmp_file_name = common::substr($4, 1, strlen($4) - 2);
      
      $$ = new ParsedSqlNode(SCF_LOAD_DATA);
      $$->load_data.relation_name = $7;
      $$->load_data.file_name = tmp_file_name;
      free($7);
      free(tmp_file_name);
    }
    ;

explain_stmt:
    EXPLAIN command_wrapper
    {
      $$ = new ParsedSqlNode(SCF_EXPLAIN);
      $$->explain.sql_node = std::unique_ptr<ParsedSqlNode>($2);
    }
    ;

set_variable_stmt:
    SET ID EQ value
    {
      $$ = new ParsedSqlNode(SCF_SET_VARIABLE);
      $$->set_variable.name  = $2;
      $$->set_variable.value = *$4;
      free($2);
      delete $4;
    }
    ;

opt_semicolon: /*empty*/
    | SEMICOLON
    ;
%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, ParsedSqlResult *sql_result) {
  yyscan_t scanner;
  yylex_init(&scanner);
  scan_string(s, scanner);
  int result = yyparse(s, sql_result, scanner);
  yylex_destroy(scanner);
  return result;
}
