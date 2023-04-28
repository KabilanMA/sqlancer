package sqlancer.stonedb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.ast.BinaryOperatorNode.Operator;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewBetweenOperatorNode;
import sqlancer.common.ast.newast.NewBinaryOperatorNode;
import sqlancer.common.ast.newast.NewCaseOperatorNode;
import sqlancer.common.ast.newast.NewFunctionNode;
import sqlancer.common.ast.newast.NewInOperatorNode;
import sqlancer.common.ast.newast.NewOrderingTerm;
import sqlancer.common.ast.newast.NewOrderingTerm.Ordering;
import sqlancer.common.ast.newast.NewTernaryNode;
import sqlancer.common.ast.newast.NewUnaryPostfixOperatorNode;
import sqlancer.common.ast.newast.NewUnaryPrefixOperatorNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.stonedb.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBCompositeDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.ast.StoneDBConstant;
import sqlancer.stonedb.ast.StoneDBExpression;

public final class StoneDBExpressionGenerator extends UntypedExpressionGenerator<Node<StoneDBExpression>, StoneDBColumn> {

    private final StoneDBGlobalState globalState;

    public StoneDBExpressionGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
    }

    private enum Expression {
        UNARY_POSTFIX, UNARY_PREFIX, BINARY_COMPARISON, BINARY_LOGICAL, BINARY_ARITHMETIC, CAST, FUNC, BETWEEN, CASE,
        IN, COLLATE, LIKE_ESCAPE
    }

    @Override
    protected Node<StoneDBExpression> generateExpression(int depth) {
        if (depth >= globalState.getOptions().getMaxExpressionDepth() || Randomly.getBoolean()) {
            return generateLeafNode();
        }
        if (allowAggregates && Randomly.getBoolean()) {
            StoneDBAggregateFunction aggregate = StoneDBAggregateFunction.getRandom();
            allowAggregates = false;
            return new NewFunctionNode<>(generateExpressions(aggregate.getNrArgs(), depth + 1), aggregate);
        }
        List<Expression> possibleOptions = new ArrayList<>(Arrays.asList(Expression.values()));
        if (!globalState.getDbmsSpecificOptions().testCollate) {
            possibleOptions.remove(Expression.COLLATE);
        }
        if (!globalState.getDbmsSpecificOptions().testFunctions) {
            possibleOptions.remove(Expression.FUNC);
        }
        if (!globalState.getDbmsSpecificOptions().testCasts) {
            possibleOptions.remove(Expression.CAST);
        }
        if (!globalState.getDbmsSpecificOptions().testBetween) {
            possibleOptions.remove(Expression.BETWEEN);
        }
        if (!globalState.getDbmsSpecificOptions().testIn) {
            possibleOptions.remove(Expression.IN);
        }
        if (!globalState.getDbmsSpecificOptions().testCase) {
            possibleOptions.remove(Expression.CASE);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryComparisons) {
            possibleOptions.remove(Expression.BINARY_COMPARISON);
        }
        if (!globalState.getDbmsSpecificOptions().testBinaryLogicals) {
            possibleOptions.remove(Expression.BINARY_LOGICAL);
        }
        Expression expr = Randomly.fromList(possibleOptions);
        switch (expr) {
        case COLLATE:
            return new NewUnaryPostfixOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    StoneDBCollate.getRandom());
        case UNARY_PREFIX:
            return new NewUnaryPrefixOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    StoneDBUnaryPrefixOperator.getRandom());
        case UNARY_POSTFIX:
            return new NewUnaryPostfixOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    StoneDBUnaryPostfixOperator.getRandom());
        case BINARY_COMPARISON:
            Operator op = StoneDBBinaryComparisonOperator.getRandom();
            return new NewBinaryOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_LOGICAL:
            op = StoneDBBinaryLogicalOperator.getRandom();
            return new NewBinaryOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), op);
        case BINARY_ARITHMETIC:
            return new NewBinaryOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), StoneDBBinaryArithmeticOperator.getRandom());
        case CAST:
            return new StoneDBCastOperation(generateExpression(depth + 1),
                    StoneDBCompositeDataType.getRandomWithoutNull());
        case FUNC:
            DBFunction func = DBFunction.getRandom();
            return new NewFunctionNode<StoneDBExpression, DBFunction>(generateExpressions(func.getNrArgs()), func);
        case BETWEEN:
            return new NewBetweenOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    generateExpression(depth + 1), generateExpression(depth + 1), Randomly.getBoolean());
        case IN:
            return new NewInOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    generateExpressions(Randomly.smallNumber() + 1, depth + 1), Randomly.getBoolean());
        case CASE:
            int nr = Randomly.smallNumber() + 1;
            return new NewCaseOperatorNode<StoneDBExpression>(generateExpression(depth + 1),
                    generateExpressions(nr, depth + 1), generateExpressions(nr, depth + 1),
                    generateExpression(depth + 1));
        case LIKE_ESCAPE:
            return new NewTernaryNode<StoneDBExpression>(generateExpression(depth + 1), generateExpression(depth + 1),
                    generateExpression(depth + 1), "LIKE", "ESCAPE");
        default:
            throw new AssertionError();
        }
    }

    @Override
    protected Node<StoneDBExpression> generateColumn() {
        StoneDBColumn column = Randomly.fromList(columns);
        return new ColumnReferenceNode<StoneDBExpression, StoneDBColumn>(column);
    }

    @Override
    public Node<StoneDBExpression> generateConstant() {
        if (Randomly.getBooleanWithSmallProbability()) {
            return StoneDBConstant.createNullConstant();
        }
        StoneDBDataType type = StoneDBDataType.getRandomWithoutNull();
        switch (type) {
        case INT:
            if (!globalState.getDbmsSpecificOptions().testIntConstants) {
                throw new IgnoreMeException();
            }
            return StoneDBConstant.createIntConstant(globalState.getRandomly().getInteger());
        case VARCHAR:
            if (!globalState.getDbmsSpecificOptions().testStringConstants) {
                throw new IgnoreMeException();
            }
            return StoneDBConstant.createStringConstant(globalState.getRandomly().getString());
        case BOOLEAN:
            if (!globalState.getDbmsSpecificOptions().testBooleanConstants) {
                throw new IgnoreMeException();
            }
            return StoneDBConstant.createBooleanConstant(Randomly.getBoolean());
        case FLOAT:
            if (!globalState.getDbmsSpecificOptions().testFloatConstants) {
                throw new IgnoreMeException();
            }
            return StoneDBConstant.createFloatConstant(globalState.getRandomly().getDouble());
        default:
            throw new AssertionError();
        }
    }

    @Override
    public List<Node<StoneDBExpression>> generateOrderBys() {
        List<Node<StoneDBExpression>> expr = super.generateOrderBys();
        List<Node<StoneDBExpression>> newExpr = new ArrayList<>(expr.size());
        for (Node<StoneDBExpression> curExpr : expr) {
            if (Randomly.getBoolean()) {
                curExpr = new NewOrderingTerm<>(curExpr, Ordering.getRandom());
            }
            newExpr.add(curExpr);
        }
        return newExpr;
    };

    public static class StoneDBCastOperation extends NewUnaryPostfixOperatorNode<StoneDBExpression> {

        public StoneDBCastOperation(Node<StoneDBExpression> expr, StoneDBCompositeDataType type) {
            super(expr, new Operator() {

                @Override
                public String getTextRepresentation() {
                    return "::" + type.toString();
                }
            });
        }

    }

    public enum StoneDBAggregateFunction {
        MAX(1), MIN(1), AVG(1), COUNT(1), STRING_AGG(1), FIRST(1), SUM(1), STDDEV_SAMP(1), STDDEV_POP(1), VAR_POP(1),
        VAR_SAMP(1), COVAR_POP(1), COVAR_SAMP(1);

        private int nrArgs;

        StoneDBAggregateFunction(int nrArgs) {
            this.nrArgs = nrArgs;
        }

        public static StoneDBAggregateFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            return nrArgs;
        }

    }

    public enum DBFunction {
        // trigonometric functions
        ACOS(1), //
        ASIN(1), //
        ATAN(1), //
        COS(1), //
        SIN(1), //
        TAN(1), //
        COT(1), //
        ATAN2(1), //
        // math functions
        ABS(1), //
        CEIL(1), //
        CEILING(1), //
        FLOOR(1), //
        LOG(1), //
        LOG10(1), LOG2(1), //
        LN(1), //
        PI(0), //
        SQRT(1), //
        POWER(1), //
        CBRT(1), //
        ROUND(2), //
        SIGN(1), //
        DEGREES(1), //
        RADIANS(1), //
        MOD(2), //
        XOR(2), //
        // string functions
        LENGTH(1), //
        LOWER(1), //
        UPPER(1), //
        SUBSTRING(3), //
        REVERSE(1), //
        CONCAT(1, true), //
        CONCAT_WS(1, true), CONTAINS(2), //
        PREFIX(2), //
        SUFFIX(2), //
        INSTR(2), //
        PRINTF(1, true), //
        REGEXP_MATCHES(2), //
        REGEXP_REPLACE(3), //
        STRIP_ACCENTS(1), //

        // date functions
        DATE_PART(2), AGE(2),

        COALESCE(3), NULLIF(2),

        // LPAD(3),
        // RPAD(3),
        LTRIM(1), RTRIM(1),
        // LEFT(2), https://github.com/cwida/duckdb/issues/633
        // REPEAT(2),
        REPLACE(3), UNICODE(1),

        BIT_COUNT(1), BIT_LENGTH(1), LAST_DAY(1), MONTHNAME(1), DAYNAME(1), YEARWEEK(1), DAYOFMONTH(1), WEEKDAY(1),
        WEEKOFYEAR(1),

        IFNULL(2), IF(3);

        private int nrArgs;
        private boolean isVariadic;

        DBFunction(int nrArgs) {
            this(nrArgs, false);
        }

        DBFunction(int nrArgs, boolean isVariadic) {
            this.nrArgs = nrArgs;
            this.isVariadic = isVariadic;
        }

        public static DBFunction getRandom() {
            return Randomly.fromOptions(values());
        }

        public int getNrArgs() {
            if (isVariadic) {
                return Randomly.smallNumber() + nrArgs;
            } else {
                return nrArgs;
            }
        }

    }

    public enum StoneDBUnaryPostfixOperator implements Operator {

        IS_NULL("IS NULL"), IS_NOT_NULL("IS NOT NULL");

        private String textRepr;

        StoneDBUnaryPostfixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static StoneDBUnaryPostfixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public static final class StoneDBCollate implements Operator {

        private final String textRepr;

        private StoneDBCollate(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return "COLLATE " + textRepr;
        }

        public static StoneDBCollate getRandom() {
            return new StoneDBCollate(StoneDBTableGenerator.getRandomCollate());
        }

    }

    public enum StoneDBUnaryPrefixOperator implements Operator {

        NOT("NOT"), PLUS("+"), MINUS("-");

        private String textRepr;

        StoneDBUnaryPrefixOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

        public static StoneDBUnaryPrefixOperator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum StoneDBBinaryLogicalOperator implements Operator {

        AND, OR;

        @Override
        public String getTextRepresentation() {
            return toString();
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

    }

    public enum StoneDBBinaryArithmeticOperator implements Operator {
        CONCAT("||"), ADD("+"), SUB("-"), MULT("*"), DIV("/"), MOD("%"), AND("&"), OR("|"), LSHIFT("<<"), RSHIFT(">>");

        private String textRepr;

        StoneDBBinaryArithmeticOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public enum StoneDBBinaryComparisonOperator implements Operator {
        EQUALS("="), GREATER(">"), GREATER_EQUALS(">="), SMALLER("<"), SMALLER_EQUALS("<="), NOT_EQUALS("!="),
        LIKE("LIKE"), NOT_LIKE("NOT LIKE"), SIMILAR_TO("SIMILAR TO"), NOT_SIMILAR_TO("NOT SIMILAR TO"),
        REGEX_POSIX("~"), REGEX_POSIT_NOT("!~");

        private String textRepr;

        StoneDBBinaryComparisonOperator(String textRepr) {
            this.textRepr = textRepr;
        }

        public static Operator getRandom() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String getTextRepresentation() {
            return textRepr;
        }

    }

    public NewFunctionNode<StoneDBExpression, StoneDBAggregateFunction> generateArgsForAggregate(
            StoneDBAggregateFunction aggregateFunction) {
        return new NewFunctionNode<StoneDBExpression, StoneDBExpressionGenerator.StoneDBAggregateFunction>(
                generateExpressions(aggregateFunction.getNrArgs()), aggregateFunction);
    }

    public Node<StoneDBExpression> generateAggregate() {
        StoneDBAggregateFunction aggrFunc = StoneDBAggregateFunction.getRandom();
        return generateArgsForAggregate(aggrFunc);
    }

    @Override
    public Node<StoneDBExpression> negatePredicate(Node<StoneDBExpression> predicate) {
        return new NewUnaryPrefixOperatorNode<>(predicate, StoneDBUnaryPrefixOperator.NOT);
    }

    @Override
    public Node<StoneDBExpression> isNull(Node<StoneDBExpression> expr) {
        return new NewUnaryPostfixOperatorNode<>(expr, StoneDBUnaryPostfixOperator.IS_NULL);
    }

}
