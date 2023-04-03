package sqlancer.stonedb.gen;

import sqlancer.Randomly;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBErrors;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBToStringVisitor;

public final class StoneDBViewGenerator {

    private StoneDBViewGenerator() {
    }

    public static SQLQueryAdapter generate(StoneDBGlobalState globalState) {
        int nrColumns = Randomly.smallNumber() + 1;
        StringBuilder sb = new StringBuilder("CREATE ");
        sb.append("VIEW ");
        sb.append(globalState.getSchema().getFreeViewName());
        sb.append("(");
        for (int i = 0; i < nrColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append("c");
            sb.append(i);
        }
        sb.append(") AS ");
        sb.append(StoneDBToStringVisitor.asString(StoneDBRandomQuerySynthesizer.generateSelect(globalState, nrColumns)));
        ExpectedErrors errors = new ExpectedErrors();
        StoneDBErrors.addExpressionErrors(errors);
        StoneDBErrors.addGroupByErrors(errors);
        return new SQLQueryAdapter(sb.toString(), errors, true);
    }

}
