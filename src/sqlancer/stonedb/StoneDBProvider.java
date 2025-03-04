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
import sqlancer.common.DBMSCommon;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLQueryProvider;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLProvider;
import sqlancer.mysql.MySQLProvider.Action;
import sqlancer.mysql.gen.MySQLTableGenerator;
import sqlancer.stonedb.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBOptions;
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

    @Override
    public void generateDatabase(StoneDBGlobalState globalState) throws Exception {
        for (int i = 0; i < Randomly.fromOptions(1, 2); i++) {
            boolean success;
            do {
            	String tableName = DBMSCommon.createTableName(globalState.getSchema().getDatabaseTables().size());
                SQLQueryAdapter qt = new StoneDBTableGenerator(globalState, tableName).generate(globalState);
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
//    	String username = "root";
    	String username = globalState.getOptions().getUserName();
//    	String password = "";
    	String password = globalState.getOptions().getPassword();
//    	String host = "localhost";
    	String host = globalState.getOptions().getHost();
    	if (host == null) {
    		host = StoneDBOptions.DEFAULT_HOST;
    	}
//    	int port = 3306;
    	int port = globalState.getOptions().getPort();
    	if (port == MainOptions.NO_SET_PORT) {
    		port = StoneDBOptions.DEFAULT_PORT;
    	}
//    	String databaseName = "test";
    	String databaseName = globalState.getDatabaseName();
//    	String mysqlSocketLocation = "/stonedb56/install/tmp/mysql.sock";
    	String mysqlSocketLocation = globalState.getSocketLocation();
    	String url = "jdbs:mysql;//"+host+":"+port+"/"+databaseName+"?unixSocket="+mysqlSocketLocation;
    	Connection conn = DriverManager.getConnection(url, username, password);
    	try (Statement s = conn.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
        }
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE DATABASE " + databaseName);
        }
        try (Statement s = conn.createStatement()) {
            s.execute("USE " + databaseName);
        }
    	return new SQLConnection(conn);
    }

    @Override
    public String getDBMSName() {
        return "stonedb";
    }

}
