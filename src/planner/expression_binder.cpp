//
// Created by JinHai on 2022/8/11.
//

#include "expression/between_expression.h"
#include "expression/in_expression.h"
#include "expression/cast_expression.h"
#include "expression/case_expression.h"
#include "expression/value_expression.h"
#include "expression/function_expression.h"
#include "expression/aggregate_expression.h"

#include "common/utility/infinity_assert.h"
#include "main/infinity.h"
#include "expression_binder.h"
#include "plan_builder.h"

namespace infinity {

std::shared_ptr<BaseExpression>
ExpressionBinder::BuildExpression(const hsql::Expr &expr, const std::shared_ptr<BindContext> &bind_context_ptr) {

    std::shared_ptr<BaseExpression> left = expr.expr ? BuildExpression(*expr.expr, bind_context_ptr) : nullptr;
    std::shared_ptr<BaseExpression> right = expr.expr2 ? BuildExpression(*expr.expr2, bind_context_ptr) : nullptr;

    LogicalType logical_type(LogicalTypeId::kNull);
    switch(expr.type) {
        case hsql::kExprLiteralFloat: {
            logical_type = LogicalType(LogicalTypeId::kDouble);
            return std::make_shared<ValueExpression>(logical_type, expr.fval);
        }
        case hsql::kExprLiteralInt: {
            // TODO: int16/int8 also can be found out.
//            logical_type = static_cast<int32_t>(expr.ival) == expr.ival ?
//                           LogicalType(LogicalTypeId::kInteger): LogicalType(LogicalTypeId::kBigInt);
            logical_type = LogicalType(LogicalTypeId::kBigInt);
            return std::make_shared<ValueExpression>(logical_type, expr.ival);
        }
        case hsql::kExprLiteralString: {
            logical_type = LogicalType(LogicalTypeId::kVarchar);
            PlannerAssert(expr.name != nullptr, "Literal string value is NULL.");
            return std::make_shared<ValueExpression>(logical_type, std::string(expr.name));
        }
        case hsql::kExprLiteralDate: {
            // TODO: transfer from date string (expr.name) to date int64
            int64_t date = 0;
            logical_type = LogicalType(LogicalTypeId::kDate);
            PlannerError("Date isn't supported during binding period.");
            return std::make_shared<ValueExpression>(logical_type, date);
        }
        case hsql::kExprLiteralInterval:
            // IntervalT should be a struct including the type of the value and an value of the interval
            // It will be bound into a ValueExpression here.
            PlannerError("Interval isn't supported during binding period.");
            break;
        case hsql::kExprLiteralNull: {
            logical_type = LogicalType(LogicalTypeId::kNull);
            return std::make_shared<ValueExpression>(logical_type);
        }
        case hsql::kExprColumnRef: {
            // TODO: Resolve column
            return BuildColExpr(expr, bind_context_ptr);
        }
        case hsql::kExprFunctionRef: {
            return BuildFuncExpr(expr, bind_context_ptr);
        }
        case hsql::kExprStar:
            PlannerError("Star expression shouldn't be bound here.");
            break;
        case hsql::kExprParameter:
            PlannerError("Used in prepare and execute? Not supported now.");
            break;
        case hsql::kExprOperator:
            return BuildOperatorExpr(expr, bind_context_ptr);
        case hsql::kExprSelect:
            // subquery expression
            return BuildSubquery(*expr.select, bind_context_ptr, SubqueryType::kScalar);
        case hsql::kExprHint:
            PlannerError("Hint isn't supported now.");
            break;
        case hsql::kExprArray:
            PlannerError("Array should be supported in IN expression.");
            break;
        case hsql::kExprArrayIndex:
            PlannerError("Array index isn't supported now.");
            break;
        case hsql::kExprExtract:
            // extract function expression
            break;
        case hsql::kExprCast:
            // cast function expression
            BuildCastExpr(expr, bind_context_ptr);
            break;
        default:
            PlannerError("Unsupported expr type");
    }

    return std::shared_ptr<BaseExpression>();
}

std::shared_ptr<BaseExpression>
ExpressionBinder::BuildColExpr(const hsql::Expr &expr, const std::shared_ptr<BindContext>& bind_context_ptr) {
    std::shared_ptr<std::string> column_name_ptr = std::make_shared<std::string>(expr.name);
    std::shared_ptr<std::string> table_name_ptr = nullptr;
    std::shared_ptr<std::string> alias_name_ptr = nullptr;
    if(expr.table != nullptr) {
        table_name_ptr = std::make_shared<std::string>(expr.table);
    }
    if(expr.alias != nullptr) {
        alias_name_ptr = std::make_shared<std::string>(expr.alias);
    }
    ColumnIdentifier column_identifier(table_name_ptr, column_name_ptr, alias_name_ptr);
    std::shared_ptr<BaseExpression> column_expr = bind_context_ptr->ResolveColumnIdentifier(column_identifier, 0);
    return column_expr;
}

std::shared_ptr<BaseExpression>
ExpressionBinder::BuildFuncExpr(const hsql::Expr &expr, const std::shared_ptr<BindContext>& bind_context_ptr) {

    std::string function_name = expr.name;

    // Transfer the function to upper case.
    std::transform(function_name.begin(), function_name.end(), function_name.begin(), [](const auto c) {
        return std::toupper(c);
    });

    // std::unique_ptr<Catalog>& catalog
    auto& catalog = Infinity::instance().catalog();
    std::shared_ptr<FunctionSet> function_set_ptr = catalog->GetFunctionSetByName(function_name);

    std::vector<std::shared_ptr<BaseExpression>> arguments;
    arguments.reserve(expr.exprList->size());
    for(const auto* arg_expr : *expr.exprList) {
        // std::shared_ptr<BaseExpression> expr_ptr
        auto expr_ptr = BuildExpression(*arg_expr, bind_context_ptr);
        arguments.emplace_back(expr_ptr);
    }

    switch(function_set_ptr->type_) {
        case FunctionType::kScalar:{
            // std::shared_ptr<ScalarFunctionSet> scalar_function_set_ptr
            auto scalar_function_set_ptr = std::static_pointer_cast<ScalarFunctionSet>(function_set_ptr);
            ScalarFunction scalar_function = scalar_function_set_ptr->GetMostMatchFunction(arguments);
            std::shared_ptr<FunctionExpression> function_expr_ptr
                = std::make_shared<FunctionExpression>(scalar_function, arguments);
            return function_expr_ptr;
        }
        case FunctionType::kAggregate: {
            // std::shared_ptr<AggregateFunctionSet> aggregate_function_set_ptr
            auto aggregate_function_set_ptr = std::static_pointer_cast<AggregateFunctionSet>(function_set_ptr);
            AggregateFunction aggregate_function = aggregate_function_set_ptr->GetMostMatchFunction(arguments);
            auto aggregate_function_ptr = std::make_shared<AggregateExpression>(aggregate_function, arguments);
            return aggregate_function_ptr;
        }
        case FunctionType::kTable:
            PlannerError("Table function shouldn't be bound here.");
        default: {
            PlannerError("Unknown function type: " + function_set_ptr->name());
        }
    }
}

std::shared_ptr<BaseExpression>
ExpressionBinder::BuildOperatorExpr(const hsql::Expr &expr, const std::shared_ptr<BindContext>& bind_context_ptr) {

    switch (expr.opType) {
            case hsql::kOpBetween: {
                std::shared_ptr<BaseExpression> value = BuildExpression(*expr.expr, bind_context_ptr);
                std::shared_ptr<BaseExpression> left_bound = BuildExpression(*(*expr.exprList)[0], bind_context_ptr);
                std::shared_ptr<BaseExpression> right_bound = BuildExpression(*(*expr.exprList)[1], bind_context_ptr);
                return std::make_shared<BetweenExpression>(value, left_bound, right_bound, true, true);
            }
            case hsql::kOpCase: {
                return BuildCaseExpr(expr, bind_context_ptr);
            }
            case hsql::kOpCaseListElement:
                PlannerError("Unexpected expression type");
            case hsql::kOpPlus: // +
                return BuildBinaryScalarExpr("+", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpMinus: // -
                return BuildBinaryScalarExpr("-", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpAsterisk: // *
                return BuildBinaryScalarExpr("*", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpSlash: // /
                return BuildBinaryScalarExpr("/", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpPercentage: // %
                return BuildBinaryScalarExpr("%", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpCaret: // ^
                return BuildBinaryScalarExpr("^", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpEquals: // =
                return BuildBinaryScalarExpr("=", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpNotEquals: // <>
                return BuildBinaryScalarExpr("<>", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpLess: // <
                return BuildBinaryScalarExpr("<", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpLessEq: // <=
                return BuildBinaryScalarExpr("<=", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpGreater: // >
                return BuildBinaryScalarExpr(">", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpGreaterEq: // >=
                return BuildBinaryScalarExpr(">=", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpLike: // like
                return BuildBinaryScalarExpr("like", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpNotLike: // not like
                return BuildBinaryScalarExpr("not like", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpILike: // ilike
                return BuildBinaryScalarExpr("ilike", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpAnd: // AND
                return BuildBinaryScalarExpr("and", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpOr: // OR
                return BuildBinaryScalarExpr("or", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpIn: { // IN
                if(expr.select != nullptr) {
                    // In subquery
                    PlannerAssert(expr.select, "No select statement in IN statement");
                    auto subquery = BuildSubquery(*expr.select, bind_context_ptr, SubqueryType::kIn);
                    subquery->left_ = BuildExpression(*expr.expr, bind_context_ptr);
                    return subquery;
                } else {
                    PlannerAssert(expr.exprList && !expr.exprList->empty(), "IN operation with emtpy list");

                    std::shared_ptr<BaseExpression> left_operand_ptr = BuildExpression(*expr.expr, bind_context_ptr);

                    std::vector<std::shared_ptr<BaseExpression>> arguments;
                    arguments.reserve(expr.exprList->size());

                    for (const auto* arg : *expr.exprList) {
                        auto arg_expr_ptr = BuildExpression(*arg, bind_context_ptr);
                        arguments.emplace_back(arg_expr_ptr);
                    }

                    return std::make_shared<InExpression>(InType::kIn, left_operand_ptr, arguments);
                }
            }
            case hsql::kOpConcat: // Concat
                return BuildBinaryScalarExpr("concat", expr.expr, expr.expr2, bind_context_ptr);
            case hsql::kOpNot: // Not
                return BuildUnaryScalarExpr("not", expr.expr, bind_context_ptr);
            case hsql::kOpUnaryMinus: // -
                return BuildUnaryScalarExpr("-", expr.expr, bind_context_ptr);
            case hsql::kOpIsNull: // IsNull
                return BuildUnaryScalarExpr("isnull", expr.expr, bind_context_ptr);
            case hsql::kOpExists: {
                // Exists
                PlannerAssert(expr.select, "No select statement in Exists");
                auto subquery = BuildSubquery(*expr.select, bind_context_ptr, SubqueryType::kExists);
                subquery->left_ = BuildExpression(*expr.expr, bind_context_ptr);
                return subquery;
            }
            default: {
                PlannerError("Unknown operator type");
            }
    }

    PlannerError("ExpressionBinder::Build Operator");
}

std::shared_ptr<BaseExpression>
ExpressionBinder::BuildCastExpr(const hsql::Expr &expr, const std::shared_ptr<BindContext>& bind_context_ptr) {
    std::shared_ptr<BaseExpression> source_expr_ptr = BuildExpression(*expr.expr, bind_context_ptr);
    LogicalType target_type = LogicalType::TypeConversion(expr.columnType.data_type);
    return CastExpression::AddCastToType(source_expr_ptr, target_type);
}

std::shared_ptr<BaseExpression>
ExpressionBinder::BuildCaseExpr(const hsql::Expr &expr, const std::shared_ptr<BindContext>& bind_context_ptr) {
    PlannerAssert(expr.exprList, "No when and then expression");
    PlannerAssert(!expr.exprList->empty(), "No when and then expression list");

    std::shared_ptr<CaseExpression> case_expression_ptr;
    // two kinds of case statement, please check:
    // https://docs.oracle.com/en/database/oracle/oracle-database/21/lnpls/CASE-statement.html

    if(expr.expr) {
        // Simple case
        std::shared_ptr<BaseExpression> left_expr_ptr = BuildExpression(*expr.expr, bind_context_ptr);

        std::string function_name = "=";
        auto &catalog = Infinity::instance().catalog();
        std::shared_ptr<FunctionSet> function_set_ptr = catalog->GetFunctionSetByName(function_name);
        auto scalar_function_set_ptr = std::static_pointer_cast<ScalarFunctionSet>(function_set_ptr);

        for (const auto *case_when_expr : *expr.exprList) {
            // Construct when expression: left_expr = value_expr
            std::vector<std::shared_ptr<BaseExpression>> arguments;
            arguments.reserve(2);
            // std::shared_ptr<BaseExpression> value_expr
            auto value_expr = BuildExpression(*case_when_expr->expr, bind_context_ptr);
            arguments.emplace_back(left_expr_ptr);
            arguments.emplace_back(value_expr);
            ScalarFunction equal_function = scalar_function_set_ptr->GetMostMatchFunction(arguments);
            std::shared_ptr<FunctionExpression> when_expr_ptr
                    = std::make_shared<FunctionExpression>(equal_function, arguments);

            // Construct then expression
            // std::shared_ptr<BaseExpression> then_expr
            std::shared_ptr<BaseExpression> then_expr_ptr = BuildExpression(*case_when_expr->expr2, bind_context_ptr);
            case_expression_ptr->AddCaseCheck(when_expr_ptr, then_expr_ptr);
        }
    } else {
        // Searched case
        for (const auto *case_when_expr : *expr.exprList) {
            // Construct when expression: left_expr = value_expr
            // std::shared_ptr<BaseExpression> when_expr
            auto when_expr_ptr = BuildExpression(*case_when_expr->expr, bind_context_ptr);

            // Construct then expression
            // std::shared_ptr<BaseExpression> then_expr
            std::shared_ptr<BaseExpression> then_expr_ptr = BuildExpression(*case_when_expr->expr2, bind_context_ptr);
            case_expression_ptr->AddCaseCheck(when_expr_ptr, then_expr_ptr);
        }
    }
    // Construct else expression
    std::shared_ptr<BaseExpression> else_expr_ptr;
    if (expr.expr2 != nullptr) {
        else_expr_ptr = BuildExpression(*expr.expr2, bind_context_ptr);
    } else {
        else_expr_ptr = std::make_shared<ValueExpression>(LogicalType(LogicalTypeId::kNull));
    }
    case_expression_ptr->AddElseExpr(else_expr_ptr);

    return case_expression_ptr;
}

std::shared_ptr<BaseExpression>
ExpressionBinder::BuildBinaryScalarExpr(const std::string& op, const hsql::Expr* left,
                                  const hsql::Expr* right, const std::shared_ptr<BindContext>& bind_context_ptr) {
    PlannerAssert(left != nullptr, "No left expression");
    PlannerAssert(right != nullptr , "No right expression");

    auto &catalog = Infinity::instance().catalog();
    std::shared_ptr<FunctionSet> function_set_ptr = catalog->GetFunctionSetByName(op);
    auto scalar_function_set_ptr = std::static_pointer_cast<ScalarFunctionSet>(function_set_ptr);

    std::vector<std::shared_ptr<BaseExpression>> arguments(2);
    arguments[0] = BuildExpression(*left, bind_context_ptr);
    arguments[1] = BuildExpression(*right, bind_context_ptr);
    ScalarFunction binary_op_function = scalar_function_set_ptr->GetMostMatchFunction(arguments);
    std::shared_ptr<FunctionExpression> result
            = std::make_shared<FunctionExpression>(binary_op_function, arguments);

    return result;
}


std::shared_ptr<BaseExpression>
ExpressionBinder::BuildUnaryScalarExpr(const std::string& op, const hsql::Expr* expr, const std::shared_ptr<BindContext>& bind_context_ptr) {

    auto &catalog = Infinity::instance().catalog();
    std::shared_ptr<FunctionSet> function_set_ptr = catalog->GetFunctionSetByName(op);
    auto scalar_function_set_ptr = std::static_pointer_cast<ScalarFunctionSet>(function_set_ptr);

    std::vector<std::shared_ptr<BaseExpression>> arguments(1);
    arguments[0] = BuildExpression(*expr, bind_context_ptr);

    ScalarFunction unary_op_function = scalar_function_set_ptr->GetMostMatchFunction(arguments);

    std::shared_ptr<FunctionExpression> result
            = std::make_shared<FunctionExpression>(unary_op_function, arguments);

    return result;
}

// Bind subquery expression.
std::shared_ptr<SubqueryExpression>
ExpressionBinder::BuildSubquery(const hsql::SelectStatement& select, const std::shared_ptr<BindContext>& bind_context_ptr, SubqueryType subquery_type) {

    std::shared_ptr<BindContext> subquery_binding_context_ptr = std::make_shared<BindContext>(bind_context_ptr);
    std::shared_ptr<BoundSelectNode> select_node_ptr
        = PlanBuilder::BuildSelect(select, subquery_binding_context_ptr);

    std::shared_ptr<SubqueryExpression> subquery_expr
        = std::make_shared<SubqueryExpression>(select_node_ptr, subquery_type);

    return subquery_expr;
}
//
//// Bind window function.
//std::shared_ptr<BaseExpression>
//ExpressionBinder::BuildWindow(const hsql::Expr &expr, const std::shared_ptr<BindContext>& bind_context_ptr) {
//    PlannerError("ExpressionBinder::BuildWindow");
//}

}