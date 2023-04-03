package sqlancer.stonedb.gen;

import java.util.List;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.AbstractUpdateGenerator;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBErrors;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;
import sqlancer.stonedb.StoneDBToStringVisitor;
import sqlancer.stonedb.ast.StoneDBExpression;

public final class StoneDBUpdateGenerator extends AbstractUpdateGenerator<StoneDBColumn> {

    private final StoneDBGlobalState globalState;
    private StoneDBExpressionGenerator gen;

    private StoneDBUpdateGenerator(StoneDBGlobalState globalState) {
        this.globalState = globalState;
    }

    public static SQLQueryAdapter getQuery(StoneDBGlobalState globalState) {
        return new StoneDBUpdateGenerator(globalState).generate();
    }

    private SQLQueryAdapter generate() {
        StoneDBTable table = globalState.getSchema().getRandomTable(t -> !t.isView());
        List<StoneDBColumn> columns = table.getRandomNonEmptyColumnSubset();
        gen = new StoneDBExpressionGenerator(globalState).setColumns(table.getColumns());
        sb.append("UPDATE ");
        sb.append(table.getName());
        sb.append(" SET ");
        updateColumns(columns);
        StoneDBErrors.addInsertErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors);
    }

    @Override
    protected void updateValue(StoneDBColumn column) {
        Node<StoneDBExpression> expr;
        if (Randomly.getBooleanWithSmallProbability()) {
            expr = gen.generateExpression();
            StoneDBErrors.addExpressionErrors(errors);
        } else {
            expr = gen.generateConstant();
        }
        sb.append(StoneDBToStringVisitor.asString(expr));
    }

}
