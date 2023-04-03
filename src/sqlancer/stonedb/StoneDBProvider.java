package sqlancer.stonedb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.google.auto.service.AutoService;

import sqlancer.AbstractAction;
import sqlancer.DatabaseProvider;
import sqlancer.IgnoreMeException;
import sqlancer.MainOptions;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.SQLProviderAdapter;
import sqlancer.StatementExecutor;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.stonedb.StoneDBProvider.StoneDBGlobalState;
import sqlancer.stonedb.gen.StoneDBDeleteGenerator;
import sqlancer.stonedb.gen.StoneDBIndexGenerator;
import sqlancer.stonedb.gen.StoneDBInsertGenerator;
import sqlancer.stonedb.gen.StoneDBRandomQuerySynthesizer;
import sqlancer.stonedb.gen.StoneDBTableGenerator;
import sqlancer.stonedb.gen.StoneDBUpdateGenerator;
import sqlancer.stonedb.gen.StoneDBViewGenerator;

@AutoService(DatabaseProvider.class)
public class StoneDBProvider extends SQLProviderAdapter<StoneDBGlobalState, StoneDBOptions> {

    public StoneDBProvider() {
        super(StoneDBGlobalState.class, StoneDBOptions.class);
    }

    public enum Action implements AbstractAction<StoneDBGlobalState> {

        INSERT(StoneDBInsertGenerator::getQuery), //
        CREATE_INDEX(StoneDBIndexGenerator::getQuery), //
        VACUUM((g) -> new SQLQueryAdapter("VACUUM;")), //
        ANALYZE((g) -> new SQLQueryAdapter("ANALYZE;")), //
        DELETE(StoneDBDeleteGenerator::generate), //
        UPDATE(StoneDBUpdateGenerator::getQuery), //
        CREATE_VIEW(StoneDBViewGenerator::generate), //
        EXPLAIN((g) -> {
            ExpectedErrors errors = new ExpectedErrors();
            StoneDBErrors.addExpressionErrors(errors);
            StoneDBErrors.addGroupByErrors(errors);
            return new SQLQueryAdapter(
                    "EXPLAIN " + StoneDBToStringVisitor
                            .asString(StoneDBRandomQuerySynthesizer.generateSelect(g, Randomly.smallNumber() + 1)),
                    errors);
        });

        private final SQLQueryProvider<StoneDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<StoneDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(StoneDBGlobalState state) throws Exception {
            return sqlQueryProvider.getQuery(state);
        }
    }

    private static int mapActions(StoneDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        switch (a) {
        case INSERT:
            return r.getInteger(0, globalState.getOptions().getMaxNumberInserts());
        case CREATE_INDEX:
            if (!globalState.getDbmsSpecificOptions().testIndexes) {
                return 0;
            }
            // fall through
        case UPDATE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumUpdates + 1);
        case VACUUM: // seems to be ignored
        case ANALYZE: // seems to be ignored
        case EXPLAIN:
            return r.getInteger(0, 2);
        case DELETE:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumDeletes + 1);
        case CREATE_VIEW:
            return r.getInteger(0, globalState.getDbmsSpecificOptions().maxNumViews + 1);
        default:
            throw new AssertionError(a);
        }
    }

    public static class StoneDBGlobalState extends SQLGlobalState<StoneDBOptions, StoneDBSchema> {

        @Override
        protected StoneDBSchema readSchema() throws SQLException {
            return StoneDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

    }

    @Override
    public void generateDatabase(StoneDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
                SQLQueryAdapter qt = new StoneDBTableGenerator().getQuery(globalState);
                success = globalState.executeStatement(qt);
            } while (!success);
        }
        if (globalState.getSchema().getDatabaseTables().isEmpty()) {
            throw new IgnoreMeException(); // TODO
        }
        StatementExecutor<StoneDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                StoneDBProvider::mapActions, (q) -> {
                    if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                        throw new IgnoreMeException();
                    }
                });
        se.executeStatements();
    }

    public void tryDeleteFile(String fname) {
        try {
            File f = new File(fname);
            f.delete();
        } catch (Exception e) {
        }
    }

    public void tryDeleteDatabase(String dbpath) {
        if (dbpath.equals("") || dbpath.equals(":memory:")) {
            return;
        }
        tryDeleteFile(dbpath);
        tryDeleteFile(dbpath + ".wal");
    }

    @Override
    public SQLConnection createDatabase(StoneDBGlobalState globalState) throws SQLException {
        String databaseFile = System.getProperty("duckdb.database.file", "");
        String url = "jdbc:duckdb:" + databaseFile;
        tryDeleteDatabase(databaseFile);

        MainOptions options = globalState.getOptions();
        if (!(options.isDefaultUsername() && options.isDefaultPassword())) {
            throw new AssertionError("DuckDB doesn't support credentials (username/password)");
        }

        Connection conn = DriverManager.getConnection(url);
        Statement stmt = conn.createStatement();
        stmt.execute("PRAGMA checkpoint_threshold='1 byte';");
        stmt.close();
        return new SQLConnection(conn);
    }

    @Override
    public String getDBMSName() {
        return "stonedb";
    }

}
