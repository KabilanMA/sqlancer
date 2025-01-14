package sqlancer.stonedb.test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.ast.newast.ColumnReferenceNode;
import sqlancer.common.ast.newast.NewPostfixTextNode;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.ast.newast.TableReferenceNode;
import sqlancer.common.oracle.NoRECBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.stonedb.StoneDBErrors;
import sqlancer.stonedb.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBCompositeDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBSchema.StoneDBTables;
import sqlancer.stonedb.StoneDBToStringVisitor;
import sqlancer.stonedb.ast.StoneDBExpression;
import sqlancer.stonedb.ast.StoneDBJoin;
import sqlancer.stonedb.ast.StoneDBSelect;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator;
import sqlancer.stonedb.gen.StoneDBExpressionGenerator.StoneDBCastOperation;

public class StoneDBNoRECOracle extends NoRECBase<StoneDBGlobalState> implements TestOracle<StoneDBGlobalState> {

    private final StoneDBSchema s;

    public StoneDBNoRECOracle(StoneDBGlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        StoneDBErrors.addExpressionErrors(errors);
    }

    @Override
    public void check() throws SQLException {
        StoneDBTables randomTables = s.getRandomTableNonEmptyTables();
        List<StoneDBColumn> columns = randomTables.getColumns();
        StoneDBExpressionGenerator gen = new StoneDBExpressionGenerator(state).setColumns(columns);
        Node<StoneDBExpression> randomWhereCondition = gen.generateExpression();
        List<StoneDBTable> tables = randomTables.getTables();
        List<TableReferenceNode<StoneDBExpression, StoneDBTable>> tableList = tables.stream()
                .map(t -> new TableReferenceNode<StoneDBExpression, StoneDBTable>(t)).collect(Collectors.toList());
        List<Node<StoneDBExpression>> joins = StoneDBJoin.getJoins(tableList, state);
        int secondCount = getSecondQuery(tableList.stream().collect(Collectors.toList()), randomWhereCondition, joins);
        int firstCount = getFirstQueryCount(con, tableList.stream().collect(Collectors.toList()), columns,
                randomWhereCondition, joins);
        if (firstCount == -1 || secondCount == -1) {
            throw new IgnoreMeException();
        }
        if (firstCount != secondCount) {
            throw new AssertionError(
                    optimizedQueryString + "; -- " + firstCount + "\n" + unoptimizedQueryString + " -- " + secondCount);
        }
    }

    private int getSecondQuery(List<Node<StoneDBExpression>> tableList, Node<StoneDBExpression> randomWhereCondition,
            List<Node<StoneDBExpression>> joins) throws SQLException {
        StoneDBSelect select = new StoneDBSelect();
        // select.setGroupByClause(groupBys);
        // DuckDBExpression isTrue = DuckDBPostfixOperation.create(randomWhereCondition,
        // PostfixOperator.IS_TRUE);
        Node<StoneDBExpression> asText = new NewPostfixTextNode<>(new StoneDBCastOperation(
                new NewPostfixTextNode<StoneDBExpression>(randomWhereCondition,
                        " IS NOT NULL AND " + StoneDBToStringVisitor.asString(randomWhereCondition)),
                new StoneDBCompositeDataType(StoneDBDataType.INT, 8)), "as count");
        select.setFetchColumns(Arrays.asList(asText));
        select.setFromList(tableList);
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int secondCount = 0;
        unoptimizedQueryString = "SELECT SUM(count) FROM (" + StoneDBToStringVisitor.asString(select) + ") as res";
        errors.add("canceling statement due to statement timeout");
        SQLQueryAdapter q = new SQLQueryAdapter(unoptimizedQueryString, errors);
        SQLancerResultSet rs;
        try {
            rs = q.executeAndGetLogged(state);
        } catch (Exception e) {
            throw new AssertionError(unoptimizedQueryString, e);
        }
        if (rs == null) {
            return -1;
        }
        if (rs.next()) {
            secondCount += rs.getLong(1);
        }
        rs.close();
        return secondCount;
    }

    private int getFirstQueryCount(SQLConnection con, List<Node<StoneDBExpression>> tableList,
            List<StoneDBColumn> columns, Node<StoneDBExpression> randomWhereCondition, List<Node<StoneDBExpression>> joins)
            throws SQLException {
        StoneDBSelect select = new StoneDBSelect();
        // select.setGroupByClause(groupBys);
        // DuckDBAggregate aggr = new DuckDBAggregate(
        List<Node<StoneDBExpression>> allColumns = columns.stream()
                .map((c) -> new ColumnReferenceNode<StoneDBExpression, StoneDBColumn>(c)).collect(Collectors.toList());
        // DuckDBAggregateFunction.COUNT);
        // select.setFetchColumns(Arrays.asList(aggr));
        select.setFetchColumns(allColumns);
        select.setFromList(tableList);
        select.setWhereClause(randomWhereCondition);
        if (Randomly.getBooleanWithSmallProbability()) {
            select.setOrderByExpressions(new StoneDBExpressionGenerator(state).setColumns(columns).generateOrderBys());
        }
        // select.setSelectType(SelectType.ALL);
        select.setJoinList(joins);
        int firstCount = 0;
        try (Statement stat = con.createStatement()) {
            optimizedQueryString = StoneDBToStringVisitor.asString(select);
            if (options.logEachSelect()) {
                logger.writeCurrent(optimizedQueryString);
            }
            try (ResultSet rs = stat.executeQuery(optimizedQueryString)) {
                while (rs.next()) {
                    firstCount++;
                }
            }
        } catch (SQLException e) {
            throw new IgnoreMeException();
        }
        return firstCount;
    }

}
