/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 2

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1




/* First part of user prologue.  */
#line 2 "yacc_sql.y"


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


#line 127 "yacc_sql.cpp"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "yacc_sql.hpp"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_SEMICOLON = 3,                  /* SEMICOLON  */
  YYSYMBOL_BY = 4,                         /* BY  */
  YYSYMBOL_CREATE = 5,                     /* CREATE  */
  YYSYMBOL_DROP = 6,                       /* DROP  */
  YYSYMBOL_GROUP = 7,                      /* GROUP  */
  YYSYMBOL_TABLE = 8,                      /* TABLE  */
  YYSYMBOL_TABLES = 9,                     /* TABLES  */
  YYSYMBOL_INDEX = 10,                     /* INDEX  */
  YYSYMBOL_CALC = 11,                      /* CALC  */
  YYSYMBOL_SELECT = 12,                    /* SELECT  */
  YYSYMBOL_DESC = 13,                      /* DESC  */
  YYSYMBOL_SHOW = 14,                      /* SHOW  */
  YYSYMBOL_SYNC = 15,                      /* SYNC  */
  YYSYMBOL_INSERT = 16,                    /* INSERT  */
  YYSYMBOL_DELETE = 17,                    /* DELETE  */
  YYSYMBOL_UPDATE = 18,                    /* UPDATE  */
  YYSYMBOL_LBRACE = 19,                    /* LBRACE  */
  YYSYMBOL_RBRACE = 20,                    /* RBRACE  */
  YYSYMBOL_COMMA = 21,                     /* COMMA  */
  YYSYMBOL_TRX_BEGIN = 22,                 /* TRX_BEGIN  */
  YYSYMBOL_TRX_COMMIT = 23,                /* TRX_COMMIT  */
  YYSYMBOL_TRX_ROLLBACK = 24,              /* TRX_ROLLBACK  */
  YYSYMBOL_INT_T = 25,                     /* INT_T  */
  YYSYMBOL_STRING_T = 26,                  /* STRING_T  */
  YYSYMBOL_DATE_T = 27,                    /* DATE_T  */
  YYSYMBOL_FLOAT_T = 28,                   /* FLOAT_T  */
  YYSYMBOL_HELP = 29,                      /* HELP  */
  YYSYMBOL_EXIT = 30,                      /* EXIT  */
  YYSYMBOL_DOT = 31,                       /* DOT  */
  YYSYMBOL_INTO = 32,                      /* INTO  */
  YYSYMBOL_VALUES = 33,                    /* VALUES  */
  YYSYMBOL_FROM = 34,                      /* FROM  */
  YYSYMBOL_WHERE = 35,                     /* WHERE  */
  YYSYMBOL_AND = 36,                       /* AND  */
  YYSYMBOL_OR = 37,                        /* OR  */
  YYSYMBOL_SET = 38,                       /* SET  */
  YYSYMBOL_ON = 39,                        /* ON  */
  YYSYMBOL_LOAD = 40,                      /* LOAD  */
  YYSYMBOL_DATA = 41,                      /* DATA  */
  YYSYMBOL_INFILE = 42,                    /* INFILE  */
  YYSYMBOL_EXPLAIN = 43,                   /* EXPLAIN  */
  YYSYMBOL_STORAGE = 44,                   /* STORAGE  */
  YYSYMBOL_FORMAT = 45,                    /* FORMAT  */
  YYSYMBOL_EQ = 46,                        /* EQ  */
  YYSYMBOL_LT = 47,                        /* LT  */
  YYSYMBOL_GT = 48,                        /* GT  */
  YYSYMBOL_LE = 49,                        /* LE  */
  YYSYMBOL_GE = 50,                        /* GE  */
  YYSYMBOL_NE = 51,                        /* NE  */
  YYSYMBOL_LIKE = 52,                      /* LIKE  */
  YYSYMBOL_IS = 53,                        /* IS  */
  YYSYMBOL_IN = 54,                        /* IN  */
  YYSYMBOL_NOT = 55,                       /* NOT  */
  YYSYMBOL_NULL_T = 56,                    /* NULL_T  */
  YYSYMBOL_INNER = 57,                     /* INNER  */
  YYSYMBOL_JOIN = 58,                      /* JOIN  */
  YYSYMBOL_HAVING = 59,                    /* HAVING  */
  YYSYMBOL_EXISTS = 60,                    /* EXISTS  */
  YYSYMBOL_UNIQUE = 61,                    /* UNIQUE  */
  YYSYMBOL_NUMBER = 62,                    /* NUMBER  */
  YYSYMBOL_FLOAT = 63,                     /* FLOAT  */
  YYSYMBOL_MAX = 64,                       /* MAX  */
  YYSYMBOL_MIN = 65,                       /* MIN  */
  YYSYMBOL_SUM = 66,                       /* SUM  */
  YYSYMBOL_AVG = 67,                       /* AVG  */
  YYSYMBOL_COUNT = 68,                     /* COUNT  */
  YYSYMBOL_ID = 69,                        /* ID  */
  YYSYMBOL_SSS = 70,                       /* SSS  */
  YYSYMBOL_DATE_STR = 71,                  /* DATE_STR  */
  YYSYMBOL_72_ = 72,                       /* '+'  */
  YYSYMBOL_73_ = 73,                       /* '-'  */
  YYSYMBOL_74_ = 74,                       /* '*'  */
  YYSYMBOL_75_ = 75,                       /* '/'  */
  YYSYMBOL_UMINUS = 76,                    /* UMINUS  */
  YYSYMBOL_YYACCEPT = 77,                  /* $accept  */
  YYSYMBOL_commands = 78,                  /* commands  */
  YYSYMBOL_command_wrapper = 79,           /* command_wrapper  */
  YYSYMBOL_exit_stmt = 80,                 /* exit_stmt  */
  YYSYMBOL_help_stmt = 81,                 /* help_stmt  */
  YYSYMBOL_sync_stmt = 82,                 /* sync_stmt  */
  YYSYMBOL_begin_stmt = 83,                /* begin_stmt  */
  YYSYMBOL_commit_stmt = 84,               /* commit_stmt  */
  YYSYMBOL_rollback_stmt = 85,             /* rollback_stmt  */
  YYSYMBOL_drop_table_stmt = 86,           /* drop_table_stmt  */
  YYSYMBOL_show_tables_stmt = 87,          /* show_tables_stmt  */
  YYSYMBOL_desc_table_stmt = 88,           /* desc_table_stmt  */
  YYSYMBOL_create_index_stmt = 89,         /* create_index_stmt  */
  YYSYMBOL_unique_op = 90,                 /* unique_op  */
  YYSYMBOL_id_list = 91,                   /* id_list  */
  YYSYMBOL_drop_index_stmt = 92,           /* drop_index_stmt  */
  YYSYMBOL_create_table_stmt = 93,         /* create_table_stmt  */
  YYSYMBOL_attr_def_list = 94,             /* attr_def_list  */
  YYSYMBOL_attr_def = 95,                  /* attr_def  */
  YYSYMBOL_number = 96,                    /* number  */
  YYSYMBOL_type = 97,                      /* type  */
  YYSYMBOL_insert_stmt = 98,               /* insert_stmt  */
  YYSYMBOL_insert_value_list = 99,         /* insert_value_list  */
  YYSYMBOL_insert_value = 100,             /* insert_value  */
  YYSYMBOL_value_list = 101,               /* value_list  */
  YYSYMBOL_value = 102,                    /* value  */
  YYSYMBOL_storage_format = 103,           /* storage_format  */
  YYSYMBOL_delete_stmt = 104,              /* delete_stmt  */
  YYSYMBOL_update_stmt = 105,              /* update_stmt  */
  YYSYMBOL_update_kv_list = 106,           /* update_kv_list  */
  YYSYMBOL_update_kv = 107,                /* update_kv  */
  YYSYMBOL_from_list = 108,                /* from_list  */
  YYSYMBOL_from_node = 109,                /* from_node  */
  YYSYMBOL_join_list = 110,                /* join_list  */
  YYSYMBOL_select_stmt = 111,              /* select_stmt  */
  YYSYMBOL_calc_stmt = 112,                /* calc_stmt  */
  YYSYMBOL_expression_list = 113,          /* expression_list  */
  YYSYMBOL_expression = 114,               /* expression  */
  YYSYMBOL_sub_query_expr = 115,           /* sub_query_expr  */
  YYSYMBOL_aggregate_type = 116,           /* aggregate_type  */
  YYSYMBOL_rel_attr = 117,                 /* rel_attr  */
  YYSYMBOL_select_attr = 118,              /* select_attr  */
  YYSYMBOL_where = 119,                    /* where  */
  YYSYMBOL_condition = 120,                /* condition  */
  YYSYMBOL_comp_op = 121,                  /* comp_op  */
  YYSYMBOL_exists_op = 122,                /* exists_op  */
  YYSYMBOL_group_by = 123,                 /* group_by  */
  YYSYMBOL_opt_having = 124,               /* opt_having  */
  YYSYMBOL_load_data_stmt = 125,           /* load_data_stmt  */
  YYSYMBOL_explain_stmt = 126,             /* explain_stmt  */
  YYSYMBOL_set_variable_stmt = 127,        /* set_variable_stmt  */
  YYSYMBOL_opt_semicolon = 128             /* opt_semicolon  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;




#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_uint8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL \
             && defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
  YYLTYPE yyls_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE) \
             + YYSIZEOF (YYLTYPE)) \
      + 2 * YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  78
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   342

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  77
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  52
/* YYNRULES -- Number of rules.  */
#define YYNRULES  133
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  241

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   327


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,    74,    72,     2,    73,     2,    75,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    76
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   224,   224,   232,   233,   234,   235,   236,   237,   238,
     239,   240,   241,   242,   243,   244,   245,   246,   247,   248,
     249,   250,   251,   255,   261,   266,   272,   278,   284,   290,
     297,   303,   311,   331,   334,   341,   344,   359,   369,   393,
     396,   409,   418,   427,   436,   445,   454,   465,   468,   469,
     470,   471,   474,   489,   492,   505,   521,   524,   535,   539,
     543,   560,   564,   573,   576,   583,   594,   617,   620,   633,
     643,   646,   657,   670,   673,   685,   717,   726,   731,   742,
     745,   748,   751,   754,   758,   763,   766,   771,   779,   789,
     792,   796,   802,   806,   813,   821,   824,   827,   830,   833,
     839,   844,   854,   859,   865,   872,   875,   881,   885,   892,
     899,   906,   910,   917,   918,   919,   920,   921,   922,   923,
     924,   925,   926,   930,   931,   937,   940,   947,   950,   957,
     970,   978,   988,   989
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "SEMICOLON", "BY",
  "CREATE", "DROP", "GROUP", "TABLE", "TABLES", "INDEX", "CALC", "SELECT",
  "DESC", "SHOW", "SYNC", "INSERT", "DELETE", "UPDATE", "LBRACE", "RBRACE",
  "COMMA", "TRX_BEGIN", "TRX_COMMIT", "TRX_ROLLBACK", "INT_T", "STRING_T",
  "DATE_T", "FLOAT_T", "HELP", "EXIT", "DOT", "INTO", "VALUES", "FROM",
  "WHERE", "AND", "OR", "SET", "ON", "LOAD", "DATA", "INFILE", "EXPLAIN",
  "STORAGE", "FORMAT", "EQ", "LT", "GT", "LE", "GE", "NE", "LIKE", "IS",
  "IN", "NOT", "NULL_T", "INNER", "JOIN", "HAVING", "EXISTS", "UNIQUE",
  "NUMBER", "FLOAT", "MAX", "MIN", "SUM", "AVG", "COUNT", "ID", "SSS",
  "DATE_STR", "'+'", "'-'", "'*'", "'/'", "UMINUS", "$accept", "commands",
  "command_wrapper", "exit_stmt", "help_stmt", "sync_stmt", "begin_stmt",
  "commit_stmt", "rollback_stmt", "drop_table_stmt", "show_tables_stmt",
  "desc_table_stmt", "create_index_stmt", "unique_op", "id_list",
  "drop_index_stmt", "create_table_stmt", "attr_def_list", "attr_def",
  "number", "type", "insert_stmt", "insert_value_list", "insert_value",
  "value_list", "value", "storage_format", "delete_stmt", "update_stmt",
  "update_kv_list", "update_kv", "from_list", "from_node", "join_list",
  "select_stmt", "calc_stmt", "expression_list", "expression",
  "sub_query_expr", "aggregate_type", "rel_attr", "select_attr", "where",
  "condition", "comp_op", "exists_op", "group_by", "opt_having",
  "load_data_stmt", "explain_stmt", "set_variable_stmt", "opt_semicolon", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-158)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-1)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int16 yypact[] =
{
     249,    -3,     1,    25,   138,   -61,    21,  -158,     6,     7,
     -26,  -158,  -158,  -158,  -158,  -158,   -18,     8,   249,    60,
      69,  -158,  -158,  -158,  -158,  -158,  -158,  -158,  -158,  -158,
    -158,  -158,  -158,  -158,  -158,  -158,  -158,  -158,  -158,  -158,
    -158,    10,  -158,    67,    17,    28,     0,  -158,  -158,  -158,
    -158,  -158,  -158,  -158,  -158,    72,  -158,  -158,    25,  -158,
    -158,  -158,   178,  -158,    89,  -158,    78,    79,  -158,    82,
    -158,  -158,    43,    50,    75,    74,    81,  -158,  -158,  -158,
    -158,   105,    63,  -158,    97,   122,    55,    77,    44,    25,
      25,    25,    25,    25,  -158,   119,   -59,    90,    96,   110,
     131,    98,    44,    99,   101,   129,   103,  -158,  -158,    25,
    -158,  -158,   267,   267,    44,    44,  -158,   158,  -158,  -158,
     117,   155,   161,    85,  -158,   145,   156,  -158,   163,   135,
     175,   128,  -158,   190,  -158,    25,   140,  -158,    96,   131,
      44,   194,   157,  -158,   247,   -20,    25,    25,    98,   131,
     208,  -158,  -158,  -158,  -158,   -16,   101,   198,   200,  -158,
     202,   154,   155,   217,   204,   161,  -158,  -158,  -158,  -158,
    -158,  -158,  -158,  -158,  -158,   -35,  -158,   -23,    25,    85,
      85,   -38,   -38,   156,  -158,   166,   164,   171,  -158,   175,
     192,   168,  -158,   199,  -158,   235,   183,    44,   223,   194,
     188,  -158,  -158,  -158,   -38,   -20,   -20,  -158,  -158,  -158,
     225,  -158,  -158,   201,  -158,   226,    85,    25,    85,  -158,
     204,  -158,  -158,  -158,    27,   210,   189,   237,   -30,  -158,
     -20,  -158,   203,  -158,   205,   226,  -158,  -158,  -158,  -158,
    -158
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_uint8 yydefact[] =
{
       0,    33,     0,     0,     0,     0,     0,    25,     0,     0,
       0,    26,    27,    28,    24,    23,     0,     0,     0,     0,
     132,    22,    21,    14,    15,    16,    17,     9,    10,    11,
      12,    13,     8,     5,     7,     6,     4,     3,    18,    19,
      20,     0,    34,     0,     0,     0,     0,    61,    58,    59,
      96,    97,    95,    98,    99,   100,    62,    60,     0,    89,
      86,    76,    77,    93,     0,    87,   100,    89,   104,     0,
      31,    30,     0,     0,     0,     0,     0,   130,     1,   133,
       2,     0,     0,    29,     0,     0,     0,     0,    85,     0,
       0,     0,     0,     0,    88,     0,     0,     0,     0,     0,
     105,     0,     0,     0,     0,     0,     0,    94,    83,     0,
     101,    78,    79,    80,    81,    82,    92,     0,   103,   102,
      73,    70,     0,     0,    65,     0,    67,   131,     0,     0,
      39,     0,    37,     0,    90,     0,     0,    72,     0,   105,
       0,    53,     0,   123,     0,   106,     0,     0,     0,   105,
       0,    48,    49,    51,    50,    44,     0,     0,     0,    84,
       0,     0,    70,   125,    56,     0,    52,   124,   113,   114,
     115,   116,   117,   118,   119,     0,   121,     0,     0,     0,
       0,   110,    69,    67,    66,     0,     0,     0,    46,    39,
      63,     0,    91,     0,    71,     0,   127,     0,     0,    53,
       0,   108,   120,   122,   107,   111,   112,    68,   129,    47,
       0,    45,    40,     0,    38,    35,     0,     0,     0,    75,
      56,    55,    54,   109,    41,     0,     0,     0,    73,   126,
     128,    57,     0,    43,     0,    35,    32,    74,    42,    64,
      36
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int16 yypgoto[] =
{
    -158,  -158,   250,  -158,  -158,  -158,  -158,  -158,  -158,  -158,
    -158,  -158,  -158,  -158,    34,  -158,  -158,    86,   114,  -158,
    -158,  -158,    83,   111,    57,   -60,  -158,  -158,  -158,   100,
     132,   123,   143,    56,   240,  -158,    -4,   -45,  -158,  -158,
    -158,  -158,  -135,  -157,  -158,  -158,  -158,  -158,  -158,  -158,
    -158,  -158
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_uint8 yydefgoto[] =
{
       0,    19,    20,    21,    22,    23,    24,    25,    26,    27,
      28,    29,    30,    43,   227,    31,    32,   157,   130,   210,
     155,    33,   166,   141,   198,    60,   214,    34,    35,   149,
     126,   139,   121,   137,    36,    37,    61,    62,    63,    64,
      65,    69,   124,   145,   178,   146,   196,   219,    38,    39,
      40,    80
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_uint8 yytable[] =
{
      68,    86,    94,   186,   163,    41,   179,   180,    70,    44,
     110,    45,     4,    88,   184,   118,   179,   180,    47,    46,
     200,   201,   205,   206,    48,    49,    94,   136,    94,   202,
      71,   203,    56,    57,    90,    91,    92,    93,    72,   187,
     188,    73,   127,    74,    46,   112,   113,   114,   115,    76,
     117,    75,    94,    94,    94,    94,    47,    94,    42,   228,
      78,   230,    48,    49,    50,    51,    52,    53,    54,    55,
      56,    57,    79,    58,    59,   108,   109,    82,   144,    81,
     164,    47,   232,   233,    94,   111,    83,    48,    49,    50,
      51,    52,    53,    54,    55,    56,    57,    84,    58,    59,
      47,   181,   182,    87,    46,   133,    48,    49,    95,    96,
      97,    47,    99,   101,    56,    57,    98,    48,    49,   100,
     102,    94,    94,   103,   104,    56,    57,    90,    91,    92,
      93,   160,   105,   204,   144,   144,   106,   220,    46,   116,
     142,    47,   107,   122,    94,   143,   110,    48,    49,    50,
      51,    52,    53,    54,    55,    56,    57,    46,    58,    59,
     151,   152,   153,   154,   119,   120,   123,   125,   131,   128,
     129,   144,   132,   144,   136,    47,   138,   148,   134,   135,
     140,    48,    49,    50,    51,    52,    53,    54,    55,    56,
      57,   147,    58,    59,    47,   150,   156,   158,   161,    89,
      48,    49,    50,    51,    52,    53,    54,    66,    56,    57,
     159,    58,    67,   229,    47,   165,   185,   167,   190,   191,
      48,    49,   192,   193,   195,   197,   209,   211,    56,    57,
      90,    91,    92,    93,    47,   208,   213,   215,   216,   217,
      48,    49,   218,   221,   223,   224,   225,   226,    56,    57,
      90,    91,    92,    93,     1,     2,   234,   236,   235,   238,
       3,     4,     5,     6,     7,     8,     9,    10,    77,   240,
     189,    11,    12,    13,   239,   212,   199,   231,    14,    15,
     183,   162,   222,   207,   237,   194,    85,    16,     0,    17,
       0,     0,    18,   168,   169,   170,   171,   172,   173,   174,
     175,   176,   177,    47,     0,     0,     0,     0,     0,    48,
      49,     0,     0,     0,     0,     0,     0,    56,    57,    90,
      91,    92,    93,    47,     0,     0,     0,     0,     0,    48,
      49,     0,     0,     0,     0,     0,     0,    56,    57,     0,
       0,    92,    93
};

static const yytype_int16 yycheck[] =
{
       4,    46,    62,    19,   139,     8,    36,    37,    69,     8,
      69,    10,    12,    58,   149,    74,    36,    37,    56,    19,
      55,    56,   179,   180,    62,    63,    86,    57,    88,    52,
       9,    54,    70,    71,    72,    73,    74,    75,    32,    55,
      56,    34,   102,    69,    19,    90,    91,    92,    93,    41,
      95,    69,   112,   113,   114,   115,    56,   117,    61,   216,
       0,   218,    62,    63,    64,    65,    66,    67,    68,    69,
      70,    71,     3,    73,    74,    20,    21,    10,   123,    69,
     140,    56,    55,    56,   144,    89,    69,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    69,    73,    74,
      56,   146,   147,    31,    19,   109,    62,    63,    19,    31,
      31,    56,    69,    38,    70,    71,    34,    62,    63,    69,
      46,   181,   182,    42,    19,    70,    71,    72,    73,    74,
      75,   135,    69,   178,   179,   180,    39,   197,    19,    20,
      55,    56,    20,    33,   204,    60,    69,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    19,    73,    74,
      25,    26,    27,    28,    74,    69,    35,    69,    39,    70,
      69,   216,    69,   218,    57,    56,    21,    21,    20,    21,
      19,    62,    63,    64,    65,    66,    67,    68,    69,    70,
      71,    46,    73,    74,    56,    32,    21,    69,    58,    21,
      62,    63,    64,    65,    66,    67,    68,    69,    70,    71,
      20,    73,    74,   217,    56,    21,     8,    60,    20,    19,
      62,    63,    20,    69,     7,    21,    62,    56,    70,    71,
      72,    73,    74,    75,    56,    69,    44,    69,    39,     4,
      62,    63,    59,    20,    56,    20,    45,    21,    70,    71,
      72,    73,    74,    75,     5,     6,    46,    20,    69,    56,
      11,    12,    13,    14,    15,    16,    17,    18,    18,   235,
     156,    22,    23,    24,    69,   189,   165,   220,    29,    30,
     148,   138,   199,   183,   228,   162,    46,    38,    -1,    40,
      -1,    -1,    43,    46,    47,    48,    49,    50,    51,    52,
      53,    54,    55,    56,    -1,    -1,    -1,    -1,    -1,    62,
      63,    -1,    -1,    -1,    -1,    -1,    -1,    70,    71,    72,
      73,    74,    75,    56,    -1,    -1,    -1,    -1,    -1,    62,
      63,    -1,    -1,    -1,    -1,    -1,    -1,    70,    71,    -1,
      -1,    74,    75
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_uint8 yystos[] =
{
       0,     5,     6,    11,    12,    13,    14,    15,    16,    17,
      18,    22,    23,    24,    29,    30,    38,    40,    43,    78,
      79,    80,    81,    82,    83,    84,    85,    86,    87,    88,
      89,    92,    93,    98,   104,   105,   111,   112,   125,   126,
     127,     8,    61,    90,     8,    10,    19,    56,    62,    63,
      64,    65,    66,    67,    68,    69,    70,    71,    73,    74,
     102,   113,   114,   115,   116,   117,    69,    74,   113,   118,
      69,     9,    32,    34,    69,    69,    41,    79,     0,     3,
     128,    69,    10,    69,    69,   111,   114,    31,   114,    21,
      72,    73,    74,    75,   102,    19,    31,    31,    34,    69,
      69,    38,    46,    42,    19,    69,    39,    20,    20,    21,
      69,   113,   114,   114,   114,   114,    20,   114,    74,    74,
      69,   109,    33,    35,   119,    69,   107,   102,    70,    69,
      95,    39,    69,   113,    20,    21,    57,   110,    21,   108,
      19,   100,    55,    60,   114,   120,   122,    46,    21,   106,
      32,    25,    26,    27,    28,    97,    21,    94,    69,    20,
     113,    58,   109,   119,   102,    21,    99,    60,    46,    47,
      48,    49,    50,    51,    52,    53,    54,    55,   121,    36,
      37,   114,   114,   107,   119,     8,    19,    55,    56,    95,
      20,    19,    20,    69,   108,     7,   123,    21,   101,   100,
      55,    56,    52,    54,   114,   120,   120,   106,    69,    62,
      96,    56,    94,    44,   103,    69,    39,     4,    59,   124,
     102,    20,    99,    56,    20,    45,    21,    91,   120,   113,
     120,   101,    55,    56,    46,    69,    20,   110,    56,    69,
      91
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_uint8 yyr1[] =
{
       0,    77,    78,    79,    79,    79,    79,    79,    79,    79,
      79,    79,    79,    79,    79,    79,    79,    79,    79,    79,
      79,    79,    79,    80,    81,    82,    83,    84,    85,    86,
      87,    88,    89,    90,    90,    91,    91,    92,    93,    94,
      94,    95,    95,    95,    95,    95,    95,    96,    97,    97,
      97,    97,    98,    99,    99,   100,   101,   101,   102,   102,
     102,   102,   102,   103,   103,   104,   105,   106,   106,   107,
     108,   108,   109,   110,   110,   111,   112,   113,   113,   114,
     114,   114,   114,   114,   114,   114,   114,   114,   114,   114,
     114,   114,   114,   114,   115,   116,   116,   116,   116,   116,
     117,   117,   118,   118,   118,   119,   119,   120,   120,   120,
     120,   120,   120,   121,   121,   121,   121,   121,   121,   121,
     121,   121,   121,   122,   122,   123,   123,   124,   124,   125,
     126,   127,   128,   128
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     2,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     3,
       2,     2,    10,     0,     1,     0,     3,     5,     8,     0,
       3,     5,     7,     6,     2,     4,     3,     1,     1,     1,
       1,     1,     6,     0,     3,     4,     0,     3,     1,     1,
       1,     1,     1,     0,     4,     4,     6,     0,     3,     3,
       0,     3,     2,     0,     6,     8,     2,     1,     3,     3,
       3,     3,     3,     3,     5,     2,     1,     1,     2,     1,
       4,     6,     3,     1,     3,     1,     1,     1,     1,     1,
       1,     3,     3,     3,     1,     0,     2,     3,     3,     4,
       2,     3,     3,     1,     1,     1,     1,     1,     1,     1,
       2,     1,     2,     1,     2,     0,     3,     0,     2,     7,
       2,     4,     0,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (&yylloc, sql_string, sql_result, scanner, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF

/* YYLLOC_DEFAULT -- Set CURRENT to span from RHS[1] to RHS[N].
   If N is 0, then set CURRENT to the empty location which ends
   the previous symbol: RHS[0] (always defined).  */

#ifndef YYLLOC_DEFAULT
# define YYLLOC_DEFAULT(Current, Rhs, N)                                \
    do                                                                  \
      if (N)                                                            \
        {                                                               \
          (Current).first_line   = YYRHSLOC (Rhs, 1).first_line;        \
          (Current).first_column = YYRHSLOC (Rhs, 1).first_column;      \
          (Current).last_line    = YYRHSLOC (Rhs, N).last_line;         \
          (Current).last_column  = YYRHSLOC (Rhs, N).last_column;       \
        }                                                               \
      else                                                              \
        {                                                               \
          (Current).first_line   = (Current).last_line   =              \
            YYRHSLOC (Rhs, 0).last_line;                                \
          (Current).first_column = (Current).last_column =              \
            YYRHSLOC (Rhs, 0).last_column;                              \
        }                                                               \
    while (0)
#endif

#define YYRHSLOC(Rhs, K) ((Rhs)[K])


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)


/* YYLOCATION_PRINT -- Print the location on the stream.
   This macro was not mandated originally: define only if we know
   we won't break user code: when these are the locations we know.  */

# ifndef YYLOCATION_PRINT

#  if defined YY_LOCATION_PRINT

   /* Temporary convenience wrapper in case some people defined the
      undocumented and private YY_LOCATION_PRINT macros.  */
#   define YYLOCATION_PRINT(File, Loc)  YY_LOCATION_PRINT(File, *(Loc))

#  elif defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL

/* Print *YYLOCP on YYO.  Private, do not rely on its existence. */

YY_ATTRIBUTE_UNUSED
static int
yy_location_print_ (FILE *yyo, YYLTYPE const * const yylocp)
{
  int res = 0;
  int end_col = 0 != yylocp->last_column ? yylocp->last_column - 1 : 0;
  if (0 <= yylocp->first_line)
    {
      res += YYFPRINTF (yyo, "%d", yylocp->first_line);
      if (0 <= yylocp->first_column)
        res += YYFPRINTF (yyo, ".%d", yylocp->first_column);
    }
  if (0 <= yylocp->last_line)
    {
      if (yylocp->first_line < yylocp->last_line)
        {
          res += YYFPRINTF (yyo, "-%d", yylocp->last_line);
          if (0 <= end_col)
            res += YYFPRINTF (yyo, ".%d", end_col);
        }
      else if (0 <= end_col && yylocp->first_column < end_col)
        res += YYFPRINTF (yyo, "-%d", end_col);
    }
  return res;
}

#   define YYLOCATION_PRINT  yy_location_print_

    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT(File, Loc)  YYLOCATION_PRINT(File, &(Loc))

#  else

#   define YYLOCATION_PRINT(File, Loc) ((void) 0)
    /* Temporary convenience wrapper in case some people defined the
       undocumented and private YY_LOCATION_PRINT macros.  */
#   define YY_LOCATION_PRINT  YYLOCATION_PRINT

#  endif
# endif /* !defined YYLOCATION_PRINT */


# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, Location, sql_string, sql_result, scanner); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, const char * sql_string, ParsedSqlResult * sql_result, void * scanner)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (yylocationp);
  YY_USE (sql_string);
  YY_USE (sql_result);
  YY_USE (scanner);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, YYLTYPE const * const yylocationp, const char * sql_string, ParsedSqlResult * sql_result, void * scanner)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  YYLOCATION_PRINT (yyo, yylocationp);
  YYFPRINTF (yyo, ": ");
  yy_symbol_value_print (yyo, yykind, yyvaluep, yylocationp, sql_string, sql_result, scanner);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp, YYLTYPE *yylsp,
                 int yyrule, const char * sql_string, ParsedSqlResult * sql_result, void * scanner)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)],
                       &(yylsp[(yyi + 1) - (yynrhs)]), sql_string, sql_result, scanner);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, yylsp, Rule, sql_string, sql_result, scanner); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
  YYLTYPE *yylloc;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif

#ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;
      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, YYLTYPE *yylocationp, const char * sql_string, ParsedSqlResult * sql_result, void * scanner)
{
  YY_USE (yyvaluep);
  YY_USE (yylocationp);
  YY_USE (sql_string);
  YY_USE (sql_result);
  YY_USE (scanner);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (const char * sql_string, ParsedSqlResult * sql_result, void * scanner)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

/* Location data for the lookahead symbol.  */
static YYLTYPE yyloc_default
# if defined YYLTYPE_IS_TRIVIAL && YYLTYPE_IS_TRIVIAL
  = { 1, 1, 1, 1 }
# endif
;
YYLTYPE yylloc = yyloc_default;

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

    /* The location stack: array, bottom, top.  */
    YYLTYPE yylsa[YYINITDEPTH];
    YYLTYPE *yyls = yylsa;
    YYLTYPE *yylsp = yyls;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;
  YYLTYPE yyloc;

  /* The locations where the error started and ended.  */
  YYLTYPE yyerror_range[3];

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N), yylsp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  yylsp[0] = yylloc;
  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;
        YYLTYPE *yyls1 = yyls;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yyls1, yysize * YYSIZEOF (*yylsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
        yyls = yyls1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
        YYSTACK_RELOCATE (yyls_alloc, yyls);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;
      yylsp = yyls + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, &yylloc, scanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      yyerror_range[1] = yylloc;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END
  *++yylsp = yylloc;

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];

  /* Default location. */
  YYLLOC_DEFAULT (yyloc, (yylsp - yylen), yylen);
  yyerror_range[1] = yyloc;
  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 2: /* commands: command_wrapper opt_semicolon  */
#line 225 "yacc_sql.y"
  {
    std::unique_ptr<ParsedSqlNode> sql_node = std::unique_ptr<ParsedSqlNode>((yyvsp[-1].sql_node));
    sql_result->add_sql_node(std::move(sql_node));
  }
#line 1847 "yacc_sql.cpp"
    break;

  case 23: /* exit_stmt: EXIT  */
#line 255 "yacc_sql.y"
         {
      (void)yynerrs;  // 这么写为了消除yynerrs未使用的告警。如果你有更好的方法欢迎提PR
      (yyval.sql_node) = new ParsedSqlNode(SCF_EXIT);
    }
#line 1856 "yacc_sql.cpp"
    break;

  case 24: /* help_stmt: HELP  */
#line 261 "yacc_sql.y"
         {
      (yyval.sql_node) = new ParsedSqlNode(SCF_HELP);
    }
#line 1864 "yacc_sql.cpp"
    break;

  case 25: /* sync_stmt: SYNC  */
#line 266 "yacc_sql.y"
         {
      (yyval.sql_node) = new ParsedSqlNode(SCF_SYNC);
    }
#line 1872 "yacc_sql.cpp"
    break;

  case 26: /* begin_stmt: TRX_BEGIN  */
#line 272 "yacc_sql.y"
               {
      (yyval.sql_node) = new ParsedSqlNode(SCF_BEGIN);
    }
#line 1880 "yacc_sql.cpp"
    break;

  case 27: /* commit_stmt: TRX_COMMIT  */
#line 278 "yacc_sql.y"
               {
      (yyval.sql_node) = new ParsedSqlNode(SCF_COMMIT);
    }
#line 1888 "yacc_sql.cpp"
    break;

  case 28: /* rollback_stmt: TRX_ROLLBACK  */
#line 284 "yacc_sql.y"
                  {
      (yyval.sql_node) = new ParsedSqlNode(SCF_ROLLBACK);
    }
#line 1896 "yacc_sql.cpp"
    break;

  case 29: /* drop_table_stmt: DROP TABLE ID  */
#line 290 "yacc_sql.y"
                  {
      (yyval.sql_node) = new ParsedSqlNode(SCF_DROP_TABLE);
      (yyval.sql_node)->drop_table.relation_name = (yyvsp[0].string);
      free((yyvsp[0].string));
    }
#line 1906 "yacc_sql.cpp"
    break;

  case 30: /* show_tables_stmt: SHOW TABLES  */
#line 297 "yacc_sql.y"
                {
      (yyval.sql_node) = new ParsedSqlNode(SCF_SHOW_TABLES);
    }
#line 1914 "yacc_sql.cpp"
    break;

  case 31: /* desc_table_stmt: DESC ID  */
#line 303 "yacc_sql.y"
             {
      (yyval.sql_node) = new ParsedSqlNode(SCF_DESC_TABLE);
      (yyval.sql_node)->desc_table.relation_name = (yyvsp[0].string);
      free((yyvsp[0].string));
    }
#line 1924 "yacc_sql.cpp"
    break;

  case 32: /* create_index_stmt: CREATE unique_op INDEX ID ON ID LBRACE ID id_list RBRACE  */
#line 312 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_CREATE_INDEX);
      CreateIndexSqlNode &create_index = (yyval.sql_node)->create_index;
      create_index.unique = (yyvsp[-8].boolean);
      create_index.index_name = (yyvsp[-6].string);
      create_index.relation_name = (yyvsp[-4].string);
      if((yyvsp[-1].relation_list) != nullptr) {
        create_index.attribute_names.swap(*(yyvsp[-1].relation_list));
        delete (yyvsp[-1].relation_list);
      }
      create_index.attribute_names.emplace_back((yyvsp[-2].string));
      std::reverse(create_index.attribute_names.begin(), create_index.attribute_names.end());
      free((yyvsp[-6].string));
      free((yyvsp[-4].string));
      free((yyvsp[-2].string));
    }
#line 1945 "yacc_sql.cpp"
    break;

  case 33: /* unique_op: %empty  */
#line 331 "yacc_sql.y"
    {
      (yyval.boolean) = false;
    }
#line 1953 "yacc_sql.cpp"
    break;

  case 34: /* unique_op: UNIQUE  */
#line 335 "yacc_sql.y"
    {
      (yyval.boolean) = true;
    }
#line 1961 "yacc_sql.cpp"
    break;

  case 35: /* id_list: %empty  */
#line 341 "yacc_sql.y"
    {
      (yyval.relation_list) = nullptr;
    }
#line 1969 "yacc_sql.cpp"
    break;

  case 36: /* id_list: COMMA ID id_list  */
#line 345 "yacc_sql.y"
    {
      if((yyvsp[0].relation_list) != nullptr)
      {
        (yyval.relation_list) = (yyvsp[0].relation_list);
      }
      else {
        (yyval.relation_list) = new std::vector<std::string>;
      }
      (yyval.relation_list)->emplace_back((yyvsp[-1].string));
      free((yyvsp[-1].string));
    }
#line 1985 "yacc_sql.cpp"
    break;

  case 37: /* drop_index_stmt: DROP INDEX ID ON ID  */
#line 360 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_DROP_INDEX);
      (yyval.sql_node)->drop_index.index_name = (yyvsp[-2].string);
      (yyval.sql_node)->drop_index.relation_name = (yyvsp[0].string);
      free((yyvsp[-2].string));
      free((yyvsp[0].string));
    }
#line 1997 "yacc_sql.cpp"
    break;

  case 38: /* create_table_stmt: CREATE TABLE ID LBRACE attr_def attr_def_list RBRACE storage_format  */
#line 370 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_CREATE_TABLE);
      CreateTableSqlNode &create_table = (yyval.sql_node)->create_table;
      create_table.relation_name = (yyvsp[-5].string);
      free((yyvsp[-5].string));

      std::vector<AttrInfoSqlNode> *src_attrs = (yyvsp[-2].attr_infos);

      if (src_attrs != nullptr) {
        create_table.attr_infos.swap(*src_attrs);
        delete src_attrs;
      }
      create_table.attr_infos.emplace_back(*(yyvsp[-3].attr_info));
      std::reverse(create_table.attr_infos.begin(), create_table.attr_infos.end());
      delete (yyvsp[-3].attr_info);
      if ((yyvsp[0].string) != nullptr) {
        create_table.storage_format = (yyvsp[0].string);
        free((yyvsp[0].string));
      }
    }
#line 2022 "yacc_sql.cpp"
    break;

  case 39: /* attr_def_list: %empty  */
#line 393 "yacc_sql.y"
    {
      (yyval.attr_infos) = nullptr;
    }
#line 2030 "yacc_sql.cpp"
    break;

  case 40: /* attr_def_list: COMMA attr_def attr_def_list  */
#line 397 "yacc_sql.y"
    {
      if ((yyvsp[0].attr_infos) != nullptr) {
        (yyval.attr_infos) = (yyvsp[0].attr_infos);
      } else {
        (yyval.attr_infos) = new std::vector<AttrInfoSqlNode>;
      }
      (yyval.attr_infos)->emplace_back(*(yyvsp[-1].attr_info));
      delete (yyvsp[-1].attr_info);
    }
#line 2044 "yacc_sql.cpp"
    break;

  case 41: /* attr_def: ID type LBRACE number RBRACE  */
#line 410 "yacc_sql.y"
    {
      (yyval.attr_info) = new AttrInfoSqlNode;
      (yyval.attr_info)->type = (AttrType)(yyvsp[-3].number);
      (yyval.attr_info)->name = (yyvsp[-4].string);
      (yyval.attr_info)->length = (yyvsp[-1].number);
      (yyval.attr_info)->nullable = false;
      free((yyvsp[-4].string));
    }
#line 2057 "yacc_sql.cpp"
    break;

  case 42: /* attr_def: ID type LBRACE number RBRACE NOT NULL_T  */
#line 419 "yacc_sql.y"
    {
      (yyval.attr_info) = new AttrInfoSqlNode;
      (yyval.attr_info)->type = (AttrType)(yyvsp[-5].number);
      (yyval.attr_info)->name = (yyvsp[-6].string);
      (yyval.attr_info)->length = (yyvsp[-3].number);
      (yyval.attr_info)->nullable = false;
      free((yyvsp[-6].string));
    }
#line 2070 "yacc_sql.cpp"
    break;

  case 43: /* attr_def: ID type LBRACE number RBRACE NULL_T  */
#line 428 "yacc_sql.y"
    {
      (yyval.attr_info) = new AttrInfoSqlNode;
      (yyval.attr_info)->type = (AttrType)(yyvsp[-4].number);
      (yyval.attr_info)->name = (yyvsp[-5].string);
      (yyval.attr_info)->length = (yyvsp[-2].number);
      (yyval.attr_info)->nullable = true;
      free((yyvsp[-5].string));
    }
#line 2083 "yacc_sql.cpp"
    break;

  case 44: /* attr_def: ID type  */
#line 437 "yacc_sql.y"
    {
      (yyval.attr_info) = new AttrInfoSqlNode;
      (yyval.attr_info)->type = (AttrType)(yyvsp[0].number);
      (yyval.attr_info)->name = (yyvsp[-1].string);
      (yyval.attr_info)->length = 4;
      (yyval.attr_info)->nullable = false;
      free((yyvsp[-1].string));
    }
#line 2096 "yacc_sql.cpp"
    break;

  case 45: /* attr_def: ID type NOT NULL_T  */
#line 446 "yacc_sql.y"
    {
      (yyval.attr_info) = new AttrInfoSqlNode;
      (yyval.attr_info)->type = (AttrType)(yyvsp[-2].number);
      (yyval.attr_info)->name = (yyvsp[-3].string);
      (yyval.attr_info)->length = 4;
      (yyval.attr_info)->nullable = false;
      free((yyvsp[-3].string));
    }
#line 2109 "yacc_sql.cpp"
    break;

  case 46: /* attr_def: ID type NULL_T  */
#line 455 "yacc_sql.y"
    {
      (yyval.attr_info) = new AttrInfoSqlNode;
      (yyval.attr_info)->type = (AttrType)(yyvsp[-1].number);
      (yyval.attr_info)->name = (yyvsp[-2].string);
      (yyval.attr_info)->length = 4;
      (yyval.attr_info)->nullable = true;
      free((yyvsp[-2].string));
    }
#line 2122 "yacc_sql.cpp"
    break;

  case 47: /* number: NUMBER  */
#line 465 "yacc_sql.y"
           {(yyval.number) = (yyvsp[0].number);}
#line 2128 "yacc_sql.cpp"
    break;

  case 48: /* type: INT_T  */
#line 468 "yacc_sql.y"
               { (yyval.number) = static_cast<int>(AttrType::INTS); }
#line 2134 "yacc_sql.cpp"
    break;

  case 49: /* type: STRING_T  */
#line 469 "yacc_sql.y"
               { (yyval.number) = static_cast<int>(AttrType::CHARS); }
#line 2140 "yacc_sql.cpp"
    break;

  case 50: /* type: FLOAT_T  */
#line 470 "yacc_sql.y"
               { (yyval.number) = static_cast<int>(AttrType::FLOATS); }
#line 2146 "yacc_sql.cpp"
    break;

  case 51: /* type: DATE_T  */
#line 471 "yacc_sql.y"
               {(yyval.number) = static_cast<int>(AttrType::DATES); }
#line 2152 "yacc_sql.cpp"
    break;

  case 52: /* insert_stmt: INSERT INTO ID VALUES insert_value insert_value_list  */
#line 475 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_INSERT);
      (yyval.sql_node)->insertion.relation_name = (yyvsp[-3].string);
      if((yyvsp[0].insert_value_list) != nullptr) {
        (yyval.sql_node)->insertion.values.swap(*(yyvsp[0].insert_value_list));
      }
      (yyval.sql_node)->insertion.values.emplace_back(*(yyvsp[-1].value_list));
      std::reverse((yyval.sql_node)->insertion.values.begin(), (yyval.sql_node)->insertion.values.end());
      delete (yyvsp[-1].value_list);
      free((yyvsp[-3].string));
    }
#line 2168 "yacc_sql.cpp"
    break;

  case 53: /* insert_value_list: %empty  */
#line 489 "yacc_sql.y"
    {
      (yyval.insert_value_list) = nullptr;
    }
#line 2176 "yacc_sql.cpp"
    break;

  case 54: /* insert_value_list: COMMA insert_value insert_value_list  */
#line 493 "yacc_sql.y"
    {
      if((yyvsp[0].insert_value_list) != nullptr) {
        (yyval.insert_value_list) = (yyvsp[0].insert_value_list);
      }
      else {
        (yyval.insert_value_list) = new std::vector<std::vector<Value>>;
      }
      (yyval.insert_value_list)->emplace_back(*(yyvsp[-1].value_list));
      delete (yyvsp[-1].value_list);
    }
#line 2191 "yacc_sql.cpp"
    break;

  case 55: /* insert_value: LBRACE value value_list RBRACE  */
#line 506 "yacc_sql.y"
    {
      if((yyvsp[-1].value_list) != nullptr) {
        (yyval.value_list) = (yyvsp[-1].value_list);
      }
      else {
        (yyval.value_list) = new std::vector<Value>;
      }
      (yyval.value_list)->emplace_back(*(yyvsp[-2].value));
      std::reverse((yyval.value_list)->begin(), (yyval.value_list)->end());
      delete (yyvsp[-2].value);
    }
#line 2207 "yacc_sql.cpp"
    break;

  case 56: /* value_list: %empty  */
#line 521 "yacc_sql.y"
    {
      (yyval.value_list) = nullptr;
    }
#line 2215 "yacc_sql.cpp"
    break;

  case 57: /* value_list: COMMA value value_list  */
#line 524 "yacc_sql.y"
                              { 
      if ((yyvsp[0].value_list) != nullptr) {
        (yyval.value_list) = (yyvsp[0].value_list);
      } else {
        (yyval.value_list) = new std::vector<Value>;
      }
      (yyval.value_list)->emplace_back(*(yyvsp[-1].value));
      delete (yyvsp[-1].value);
    }
#line 2229 "yacc_sql.cpp"
    break;

  case 58: /* value: NUMBER  */
#line 535 "yacc_sql.y"
           {
      (yyval.value) = new Value((int)(yyvsp[0].number));
      (yyloc) = (yylsp[0]);
    }
#line 2238 "yacc_sql.cpp"
    break;

  case 59: /* value: FLOAT  */
#line 539 "yacc_sql.y"
           {
      (yyval.value) = new Value((float)(yyvsp[0].floats));
      (yyloc) = (yylsp[0]);
    }
#line 2247 "yacc_sql.cpp"
    break;

  case 60: /* value: DATE_STR  */
#line 543 "yacc_sql.y"
               {
      char *tmp = common::substr((yyvsp[0].string),1,strlen((yyvsp[0].string))-2);
      std::string str(tmp);
      Value *value = new Value();
      int date;
      if(common::string_to_date(str, date) < 0)
      {
        free(tmp);
        free((yyvsp[0].string));
        yyerror(&(yyloc), sql_string, sql_result, scanner, "error", true);
        YYERROR;
      }
      value->set_date(date);
      (yyval.value) = value;
      free(tmp);
      free((yyvsp[0].string));
    }
#line 2269 "yacc_sql.cpp"
    break;

  case 61: /* value: NULL_T  */
#line 560 "yacc_sql.y"
            {
      (yyval.value) = new Value();
      (yyval.value)->set_null();
    }
#line 2278 "yacc_sql.cpp"
    break;

  case 62: /* value: SSS  */
#line 564 "yacc_sql.y"
         {
      char *tmp = common::substr((yyvsp[0].string),1,strlen((yyvsp[0].string))-2);
      (yyval.value) = new Value(tmp);
      free(tmp);
      free((yyvsp[0].string));
    }
#line 2289 "yacc_sql.cpp"
    break;

  case 63: /* storage_format: %empty  */
#line 573 "yacc_sql.y"
    {
      (yyval.string) = nullptr;
    }
#line 2297 "yacc_sql.cpp"
    break;

  case 64: /* storage_format: STORAGE FORMAT EQ ID  */
#line 577 "yacc_sql.y"
    {
      (yyval.string) = (yyvsp[0].string);
    }
#line 2305 "yacc_sql.cpp"
    break;

  case 65: /* delete_stmt: DELETE FROM ID where  */
#line 584 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_DELETE);
      (yyval.sql_node)->deletion.relation_name = (yyvsp[-1].string);
      if ((yyvsp[0].expression) != nullptr) {
        (yyval.sql_node)->deletion.conditions = (yyvsp[0].expression);
      }
      free((yyvsp[-1].string));
    }
#line 2318 "yacc_sql.cpp"
    break;

  case 66: /* update_stmt: UPDATE ID SET update_kv update_kv_list where  */
#line 595 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_UPDATE);
      (yyval.sql_node)->update.relation_name = (yyvsp[-4].string);
      (yyval.sql_node)->update.attribute_names.emplace_back((yyvsp[-2].update_kv)->attribute_name);
      (yyval.sql_node)->update.values.emplace_back((yyvsp[-2].update_kv)->value);
      if((yyvsp[-1].update_kv_list) != nullptr) {
        for(UpdateKv kv : *(yyvsp[-1].update_kv_list)) {
          (yyval.sql_node)->update.attribute_names.emplace_back(kv.attribute_name);
          (yyval.sql_node)->update.values.emplace_back(kv.value);
        }
        delete (yyvsp[-1].update_kv_list);
      }
      if((yyvsp[0].expression) != nullptr) {
        (yyval.sql_node)->update.conditions = (yyvsp[0].expression);
      }
      free((yyvsp[-4].string));
      delete (yyvsp[-2].update_kv);
    }
#line 2341 "yacc_sql.cpp"
    break;

  case 67: /* update_kv_list: %empty  */
#line 617 "yacc_sql.y"
    {
      (yyval.update_kv_list) = nullptr;
    }
#line 2349 "yacc_sql.cpp"
    break;

  case 68: /* update_kv_list: COMMA update_kv update_kv_list  */
#line 621 "yacc_sql.y"
    {
      if((yyvsp[0].update_kv_list) != nullptr) {
        (yyval.update_kv_list) = (yyvsp[0].update_kv_list);
      } else {
        (yyval.update_kv_list) = new std::vector<UpdateKv>;
      }
      (yyval.update_kv_list)->emplace_back(*(yyvsp[-1].update_kv));
      delete (yyvsp[-1].update_kv);
    }
#line 2363 "yacc_sql.cpp"
    break;

  case 69: /* update_kv: ID EQ expression  */
#line 634 "yacc_sql.y"
    {
      (yyval.update_kv) = new UpdateKv;
      (yyval.update_kv)->attribute_name = (yyvsp[-2].string);
      (yyval.update_kv)->value = (yyvsp[0].expression);
      free((yyvsp[-2].string));
    }
#line 2374 "yacc_sql.cpp"
    break;

  case 70: /* from_list: %empty  */
#line 643 "yacc_sql.y"
                {
      (yyval.inner_joins_list) = nullptr;
    }
#line 2382 "yacc_sql.cpp"
    break;

  case 71: /* from_list: COMMA from_node from_list  */
#line 646 "yacc_sql.y"
                                {
      if((yyvsp[0].inner_joins_list) != nullptr) {
        (yyval.inner_joins_list) = (yyvsp[0].inner_joins_list);
      } else {
        (yyval.inner_joins_list) = new std::vector<InnerJoinSqlNode>;
      }
      (yyval.inner_joins_list)->emplace_back(*(yyvsp[-1].inner_joins));
      delete (yyvsp[-1].inner_joins);
    }
#line 2396 "yacc_sql.cpp"
    break;

  case 72: /* from_node: ID join_list  */
#line 657 "yacc_sql.y"
                 {
      if((yyvsp[0].inner_joins) != nullptr) {
        (yyval.inner_joins) = (yyvsp[0].inner_joins);
      } else {
        (yyval.inner_joins) = new InnerJoinSqlNode;
      }
      (yyval.inner_joins)->base_relation = (yyvsp[-1].string);
      std::reverse((yyval.inner_joins)->join_relations.begin(), (yyval.inner_joins)->join_relations.end());
      std::reverse((yyval.inner_joins)->conditions.begin(), (yyval.inner_joins)->conditions.end());
      free((yyvsp[-1].string));
    }
#line 2412 "yacc_sql.cpp"
    break;

  case 73: /* join_list: %empty  */
#line 670 "yacc_sql.y"
                {
      (yyval.inner_joins) = nullptr;
    }
#line 2420 "yacc_sql.cpp"
    break;

  case 74: /* join_list: INNER JOIN ID ON condition join_list  */
#line 673 "yacc_sql.y"
                                           {
      if((yyvsp[0].inner_joins) != nullptr) {
        (yyval.inner_joins) = (yyvsp[0].inner_joins);
      } else {
        (yyval.inner_joins) = new InnerJoinSqlNode;
      }
      (yyval.inner_joins)->join_relations.emplace_back((yyvsp[-3].string));
      (yyval.inner_joins)->conditions.emplace_back((yyvsp[-1].expression));
      free((yyvsp[-3].string));
    }
#line 2435 "yacc_sql.cpp"
    break;

  case 75: /* select_stmt: SELECT select_attr FROM from_node from_list where group_by opt_having  */
#line 686 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_SELECT);
      if ((yyvsp[-6].expression_list) != nullptr) {
        (yyval.sql_node)->selection.expressions.swap(*(yyvsp[-6].expression_list));
        delete (yyvsp[-6].expression_list);
      }

      if((yyvsp[-3].inner_joins_list) != nullptr) {
        (yyval.sql_node)->selection.relations.swap(*(yyvsp[-3].inner_joins_list));
        delete (yyvsp[-3].inner_joins_list);
      }
      (yyval.sql_node)->selection.relations.emplace_back(*(yyvsp[-4].inner_joins));
      std::reverse((yyval.sql_node)->selection.relations.begin(), (yyval.sql_node)->selection.relations.end());

      if ((yyvsp[-2].expression) != nullptr) {
        (yyval.sql_node)->selection.conditions = (yyvsp[-2].expression);
      }

      if ((yyvsp[-1].expression_list) != nullptr) {
        (yyval.sql_node)->selection.group_by.swap(*(yyvsp[-1].expression_list));
        delete (yyvsp[-1].expression_list);
      }

      if((yyvsp[0].expression) != nullptr) {
        (yyval.sql_node)->selection.having_conditions = (yyvsp[0].expression);
      }

      delete (yyvsp[-4].inner_joins);
    }
#line 2469 "yacc_sql.cpp"
    break;

  case 76: /* calc_stmt: CALC expression_list  */
#line 718 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_CALC);
      (yyval.sql_node)->calc.expressions.swap(*(yyvsp[0].expression_list));
      delete (yyvsp[0].expression_list);
    }
#line 2479 "yacc_sql.cpp"
    break;

  case 77: /* expression_list: expression  */
#line 727 "yacc_sql.y"
    {
      (yyval.expression_list) = new std::vector<std::unique_ptr<Expression>>;
      (yyval.expression_list)->emplace_back((yyvsp[0].expression));
    }
#line 2488 "yacc_sql.cpp"
    break;

  case 78: /* expression_list: expression COMMA expression_list  */
#line 732 "yacc_sql.y"
    {
      if ((yyvsp[0].expression_list) != nullptr) {
        (yyval.expression_list) = (yyvsp[0].expression_list);
      } else {
        (yyval.expression_list) = new std::vector<std::unique_ptr<Expression>>;
      }
      (yyval.expression_list)->emplace((yyval.expression_list)->begin(), (yyvsp[-2].expression));
    }
#line 2501 "yacc_sql.cpp"
    break;

  case 79: /* expression: expression '+' expression  */
#line 742 "yacc_sql.y"
                              {
      (yyval.expression) = create_arithmetic_expression(ArithmeticExpr::Type::ADD, (yyvsp[-2].expression), (yyvsp[0].expression), sql_string, &(yyloc));
    }
#line 2509 "yacc_sql.cpp"
    break;

  case 80: /* expression: expression '-' expression  */
#line 745 "yacc_sql.y"
                                {
      (yyval.expression) = create_arithmetic_expression(ArithmeticExpr::Type::SUB, (yyvsp[-2].expression), (yyvsp[0].expression), sql_string, &(yyloc));
    }
#line 2517 "yacc_sql.cpp"
    break;

  case 81: /* expression: expression '*' expression  */
#line 748 "yacc_sql.y"
                                {
      (yyval.expression) = create_arithmetic_expression(ArithmeticExpr::Type::MUL, (yyvsp[-2].expression), (yyvsp[0].expression), sql_string, &(yyloc));
    }
#line 2525 "yacc_sql.cpp"
    break;

  case 82: /* expression: expression '/' expression  */
#line 751 "yacc_sql.y"
                                {
      (yyval.expression) = create_arithmetic_expression(ArithmeticExpr::Type::DIV, (yyvsp[-2].expression), (yyvsp[0].expression), sql_string, &(yyloc));
    }
#line 2533 "yacc_sql.cpp"
    break;

  case 83: /* expression: LBRACE expression RBRACE  */
#line 754 "yacc_sql.y"
                               {
      (yyval.expression) = (yyvsp[-1].expression);
      (yyval.expression)->set_name(token_name(sql_string, &(yyloc)));
    }
#line 2542 "yacc_sql.cpp"
    break;

  case 84: /* expression: LBRACE expression COMMA expression_list RBRACE  */
#line 758 "yacc_sql.y"
                                                     {
      (yyval.expression) = new ListExpr((yyvsp[-3].expression), std::move(*(yyvsp[-1].expression_list)));
      (yyval.expression)->set_name(token_name(sql_string, &(yyloc)));
      delete (yyvsp[-1].expression_list);
    }
#line 2552 "yacc_sql.cpp"
    break;

  case 85: /* expression: '-' expression  */
#line 763 "yacc_sql.y"
                                  {
      (yyval.expression) = create_arithmetic_expression(ArithmeticExpr::Type::NEGATIVE, (yyvsp[0].expression), nullptr, sql_string, &(yyloc));
    }
#line 2560 "yacc_sql.cpp"
    break;

  case 86: /* expression: value  */
#line 766 "yacc_sql.y"
            {
      (yyval.expression) = new ValueExpr(*(yyvsp[0].value));
      (yyval.expression)->set_name(token_name(sql_string, &(yyloc)));
      delete (yyvsp[0].value);
    }
#line 2570 "yacc_sql.cpp"
    break;

  case 87: /* expression: rel_attr  */
#line 771 "yacc_sql.y"
               {
      FieldExpr *tmp = new FieldExpr();
      tmp->set_table_name((yyvsp[0].rel_attr)->relation_name);
      tmp->set_field_name((yyvsp[0].rel_attr)->attribute_name);
      (yyval.expression) = tmp;
      (yyval.expression)->set_name(token_name(sql_string, &(yyloc)));
      delete (yyvsp[0].rel_attr);
    }
#line 2583 "yacc_sql.cpp"
    break;

  case 88: /* expression: expression value  */
#line 779 "yacc_sql.y"
                       {
      if(!(yyvsp[0].value)->is_minus())
      {
        yyerror(&(yyloc), sql_string, sql_result, scanner, "error", false);
        YYERROR;
      }
      ValueExpr *val = new ValueExpr(*(yyvsp[0].value));
      (yyval.expression) = create_arithmetic_expression(ArithmeticExpr::Type::ADD, (yyvsp[-1].expression), val, sql_string, &(yyloc));
      delete (yyvsp[0].value);
    }
#line 2598 "yacc_sql.cpp"
    break;

  case 89: /* expression: '*'  */
#line 789 "yacc_sql.y"
          {
      (yyval.expression) = new StarExpr();
    }
#line 2606 "yacc_sql.cpp"
    break;

  case 90: /* expression: aggregate_type LBRACE expression RBRACE  */
#line 792 "yacc_sql.y"
                                              {
      (yyval.expression) = create_aggregate_expression((yyvsp[-3].string), (yyvsp[-1].expression), sql_string, &(yyloc));
      free((yyvsp[-3].string));
    }
#line 2615 "yacc_sql.cpp"
    break;

  case 91: /* expression: aggregate_type LBRACE expression COMMA expression_list RBRACE  */
#line 796 "yacc_sql.y"
                                                                    {
      (yyval.expression) = create_aggregate_expression("MAX", new StarExpr(), sql_string, &(yyloc));
      free((yyvsp[-5].string));
      delete (yyvsp[-3].expression);
      delete (yyvsp[-1].expression_list);
    }
#line 2626 "yacc_sql.cpp"
    break;

  case 92: /* expression: aggregate_type LBRACE RBRACE  */
#line 802 "yacc_sql.y"
                                   {
      (yyval.expression) = create_aggregate_expression("MAX", new StarExpr(), sql_string, &(yyloc));
      free((yyvsp[-2].string));
    }
#line 2635 "yacc_sql.cpp"
    break;

  case 93: /* expression: sub_query_expr  */
#line 806 "yacc_sql.y"
                     {
      (yyval.expression) = (yyvsp[0].expression);
    }
#line 2643 "yacc_sql.cpp"
    break;

  case 94: /* sub_query_expr: LBRACE select_stmt RBRACE  */
#line 814 "yacc_sql.y"
    {
      (yyval.expression) = new SubQueryExpr((yyvsp[-1].sql_node)->selection);
      delete (yyvsp[-1].sql_node);
    }
#line 2652 "yacc_sql.cpp"
    break;

  case 95: /* aggregate_type: SUM  */
#line 821 "yacc_sql.y"
        {
      (yyval.string) = (yyvsp[0].string);
    }
#line 2660 "yacc_sql.cpp"
    break;

  case 96: /* aggregate_type: MAX  */
#line 824 "yacc_sql.y"
          {
      (yyval.string) = (yyvsp[0].string);
    }
#line 2668 "yacc_sql.cpp"
    break;

  case 97: /* aggregate_type: MIN  */
#line 827 "yacc_sql.y"
          {
      (yyval.string) = (yyvsp[0].string);
    }
#line 2676 "yacc_sql.cpp"
    break;

  case 98: /* aggregate_type: AVG  */
#line 830 "yacc_sql.y"
          {
      (yyval.string) = (yyvsp[0].string);
    }
#line 2684 "yacc_sql.cpp"
    break;

  case 99: /* aggregate_type: COUNT  */
#line 833 "yacc_sql.y"
            {
      (yyval.string) = (yyvsp[0].string);
    }
#line 2692 "yacc_sql.cpp"
    break;

  case 100: /* rel_attr: ID  */
#line 839 "yacc_sql.y"
       {
      (yyval.rel_attr) = new RelAttrSqlNode;
      (yyval.rel_attr)->attribute_name = (yyvsp[0].string);
      free((yyvsp[0].string));
    }
#line 2702 "yacc_sql.cpp"
    break;

  case 101: /* rel_attr: ID DOT ID  */
#line 844 "yacc_sql.y"
                {
      (yyval.rel_attr) = new RelAttrSqlNode;
      (yyval.rel_attr)->relation_name  = (yyvsp[-2].string);
      (yyval.rel_attr)->attribute_name = (yyvsp[0].string);
      free((yyvsp[-2].string));
      free((yyvsp[0].string));
    }
#line 2714 "yacc_sql.cpp"
    break;

  case 102: /* select_attr: '*' DOT '*'  */
#line 854 "yacc_sql.y"
                  {
        (yyval.expression_list) = new std::vector<std::unique_ptr<Expression>>;
        StarExpr *expr = new StarExpr("*");
        (yyval.expression_list)->emplace_back(expr);
      }
#line 2724 "yacc_sql.cpp"
    break;

  case 103: /* select_attr: ID DOT '*'  */
#line 859 "yacc_sql.y"
                   {
        (yyval.expression_list) = new std::vector<std::unique_ptr<Expression>>;
        StarExpr *expr = new StarExpr((yyvsp[-2].string));
        (yyval.expression_list)->emplace_back(expr);
        free((yyvsp[-2].string));
      }
#line 2735 "yacc_sql.cpp"
    break;

  case 104: /* select_attr: expression_list  */
#line 865 "yacc_sql.y"
                        {
        (yyval.expression_list) = (yyvsp[0].expression_list);
      }
#line 2743 "yacc_sql.cpp"
    break;

  case 105: /* where: %empty  */
#line 872 "yacc_sql.y"
    {
      (yyval.expression) = nullptr;
    }
#line 2751 "yacc_sql.cpp"
    break;

  case 106: /* where: WHERE condition  */
#line 875 "yacc_sql.y"
                      {
      (yyval.expression) = (yyvsp[0].expression);  
    }
#line 2759 "yacc_sql.cpp"
    break;

  case 107: /* condition: expression comp_op expression  */
#line 882 "yacc_sql.y"
    {
      (yyval.expression) = new ComparisonExpr((yyvsp[-1].comp), (yyvsp[-2].expression), (yyvsp[0].expression));
    }
#line 2767 "yacc_sql.cpp"
    break;

  case 108: /* condition: expression IS NULL_T  */
#line 886 "yacc_sql.y"
    {
      Value val;
      val.set_null();
      ValueExpr *value_expr = new ValueExpr(val);
      (yyval.expression) = new ComparisonExpr(IS_NULL, (yyvsp[-2].expression), value_expr);
    }
#line 2778 "yacc_sql.cpp"
    break;

  case 109: /* condition: expression IS NOT NULL_T  */
#line 893 "yacc_sql.y"
    {
      Value val;
      val.set_null();
      ValueExpr *value_expr = new ValueExpr(val);
      (yyval.expression) = new ComparisonExpr(IS_NULL, (yyvsp[-3].expression), value_expr);
    }
#line 2789 "yacc_sql.cpp"
    break;

  case 110: /* condition: exists_op expression  */
#line 900 "yacc_sql.y"
    {
      Value val;
      val.set_null();
      ValueExpr *value_expr = new ValueExpr(val);
      (yyval.expression) = new ComparisonExpr((yyvsp[-1].comp), value_expr, (yyvsp[0].expression));
    }
#line 2800 "yacc_sql.cpp"
    break;

  case 111: /* condition: condition AND condition  */
#line 907 "yacc_sql.y"
    {
      (yyval.expression) = new ConjunctionExpr(ConjunctionExpr::Type::AND, (yyvsp[-2].expression), (yyvsp[0].expression));
    }
#line 2808 "yacc_sql.cpp"
    break;

  case 112: /* condition: condition OR condition  */
#line 911 "yacc_sql.y"
    {
      (yyval.expression) = new ConjunctionExpr(ConjunctionExpr::Type::OR, (yyvsp[-2].expression), (yyvsp[0].expression));
    }
#line 2816 "yacc_sql.cpp"
    break;

  case 113: /* comp_op: EQ  */
#line 917 "yacc_sql.y"
         { (yyval.comp) = EQUAL_TO; }
#line 2822 "yacc_sql.cpp"
    break;

  case 114: /* comp_op: LT  */
#line 918 "yacc_sql.y"
         { (yyval.comp) = LESS_THAN; }
#line 2828 "yacc_sql.cpp"
    break;

  case 115: /* comp_op: GT  */
#line 919 "yacc_sql.y"
         { (yyval.comp) = GREAT_THAN; }
#line 2834 "yacc_sql.cpp"
    break;

  case 116: /* comp_op: LE  */
#line 920 "yacc_sql.y"
         { (yyval.comp) = LESS_EQUAL; }
#line 2840 "yacc_sql.cpp"
    break;

  case 117: /* comp_op: GE  */
#line 921 "yacc_sql.y"
         { (yyval.comp) = GREAT_EQUAL; }
#line 2846 "yacc_sql.cpp"
    break;

  case 118: /* comp_op: NE  */
#line 922 "yacc_sql.y"
         { (yyval.comp) = NOT_EQUAL; }
#line 2852 "yacc_sql.cpp"
    break;

  case 119: /* comp_op: LIKE  */
#line 923 "yacc_sql.y"
           { (yyval.comp) = LIKE_OP; }
#line 2858 "yacc_sql.cpp"
    break;

  case 120: /* comp_op: NOT LIKE  */
#line 924 "yacc_sql.y"
               { (yyval.comp) = NOT_LIKE_OP; }
#line 2864 "yacc_sql.cpp"
    break;

  case 121: /* comp_op: IN  */
#line 925 "yacc_sql.y"
         { (yyval.comp) = IN_OP; }
#line 2870 "yacc_sql.cpp"
    break;

  case 122: /* comp_op: NOT IN  */
#line 926 "yacc_sql.y"
             { (yyval.comp) = NOT_IN_OP; }
#line 2876 "yacc_sql.cpp"
    break;

  case 123: /* exists_op: EXISTS  */
#line 930 "yacc_sql.y"
           { (yyval.comp) = EXISTS_OP; }
#line 2882 "yacc_sql.cpp"
    break;

  case 124: /* exists_op: NOT EXISTS  */
#line 931 "yacc_sql.y"
                 { (yyval.comp) = NOT_EXISTS_OP; }
#line 2888 "yacc_sql.cpp"
    break;

  case 125: /* group_by: %empty  */
#line 937 "yacc_sql.y"
    {
      (yyval.expression_list) = nullptr;
    }
#line 2896 "yacc_sql.cpp"
    break;

  case 126: /* group_by: GROUP BY expression_list  */
#line 941 "yacc_sql.y"
    {
      (yyval.expression_list) = (yyvsp[0].expression_list);
    }
#line 2904 "yacc_sql.cpp"
    break;

  case 127: /* opt_having: %empty  */
#line 947 "yacc_sql.y"
                {
      (yyval.expression) = nullptr;
    }
#line 2912 "yacc_sql.cpp"
    break;

  case 128: /* opt_having: HAVING condition  */
#line 951 "yacc_sql.y"
    {
      (yyval.expression) = (yyvsp[0].expression);
    }
#line 2920 "yacc_sql.cpp"
    break;

  case 129: /* load_data_stmt: LOAD DATA INFILE SSS INTO TABLE ID  */
#line 958 "yacc_sql.y"
    {
      char *tmp_file_name = common::substr((yyvsp[-3].string), 1, strlen((yyvsp[-3].string)) - 2);
      
      (yyval.sql_node) = new ParsedSqlNode(SCF_LOAD_DATA);
      (yyval.sql_node)->load_data.relation_name = (yyvsp[0].string);
      (yyval.sql_node)->load_data.file_name = tmp_file_name;
      free((yyvsp[0].string));
      free(tmp_file_name);
    }
#line 2934 "yacc_sql.cpp"
    break;

  case 130: /* explain_stmt: EXPLAIN command_wrapper  */
#line 971 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_EXPLAIN);
      (yyval.sql_node)->explain.sql_node = std::unique_ptr<ParsedSqlNode>((yyvsp[0].sql_node));
    }
#line 2943 "yacc_sql.cpp"
    break;

  case 131: /* set_variable_stmt: SET ID EQ value  */
#line 979 "yacc_sql.y"
    {
      (yyval.sql_node) = new ParsedSqlNode(SCF_SET_VARIABLE);
      (yyval.sql_node)->set_variable.name  = (yyvsp[-2].string);
      (yyval.sql_node)->set_variable.value = *(yyvsp[0].value);
      free((yyvsp[-2].string));
      delete (yyvsp[0].value);
    }
#line 2955 "yacc_sql.cpp"
    break;


#line 2959 "yacc_sql.cpp"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;
  *++yylsp = yyloc;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken, &yylloc};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (&yylloc, sql_string, sql_result, scanner, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
    }

  yyerror_range[1] = yylloc;
  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, &yylloc, sql_string, sql_result, scanner);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;

      yyerror_range[1] = *yylsp;
      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, yylsp, sql_string, sql_result, scanner);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  yyerror_range[2] = yylloc;
  ++yylsp;
  YYLLOC_DEFAULT (*yylsp, yyerror_range, 2);

  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (&yylloc, sql_string, sql_result, scanner, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, &yylloc, sql_string, sql_result, scanner);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, yylsp, sql_string, sql_result, scanner);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}

#line 991 "yacc_sql.y"

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
