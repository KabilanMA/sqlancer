package sqlancer.stonedb;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.SQLConnection;
import sqlancer.common.DBMSCommon;
import sqlancer.common.schema.AbstractRelationalTable;
import sqlancer.common.schema.AbstractSchema;
import sqlancer.common.schema.AbstractTableColumn;
import sqlancer.common.schema.AbstractTables;
import sqlancer.common.schema.TableIndex;
import sqlancer.mysql.MySQLGlobalState;
import sqlancer.mysql.MySQLSchema.MySQLDataType;
import sqlancer.stonedb.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema.StoneDBTable;

public class StoneDBSchema extends AbstractSchema<StoneDBGlobalState, StoneDBTable> {

    public enum StoneDBDataType {

        INT, VARCHAR, BOOLEAN, DOUBLE, FLOAT, DECIMAL, NULL;

        public static StoneDBDataType getRandomWithoutNull() {
            StoneDBDataType dt;
            do {
                dt = Randomly.fromOptions(values());
            } while (dt == StoneDBDataType.NULL);
            return dt;
        }

        public boolean isNumeric() {
            switch (this) {
            case INT:
            case DOUBLE:
            case DECIMAL:
            case FLOAT:
                return true;
            case VARCHAR:
            case BOOLEAN:
            case NULL:
                return false;
            default:
                throw new AssertionError(this);
            }
        }

    }

    public static class StoneDBCompositeDataType {

        private final StoneDBDataType dataType;

        private final int size;

        public StoneDBCompositeDataType(StoneDBDataType dataType, int size) {
            this.dataType = dataType;
            this.size = size;
        }

        public StoneDBDataType getPrimitiveDataType() {
            return dataType;
        }

        public int getSize() {
            if (size == -1) {
                throw new AssertionError(this);
            }
            return size;
        }

        public static StoneDBCompositeDataType getRandomWithoutNull() {
            StoneDBDataType type = StoneDBDataType.getRandomWithoutNull();
            int size = -1;
            switch (type) {
            case INT:
                size = Randomly.fromOptions(1, 2, 4, 8);
                break;
            case FLOAT:
                size = Randomly.fromOptions(4, 8);
                break;
            case BOOLEAN:
            case VARCHAR:
                size = 0;
                break;
            default:
                throw new AssertionError(type);
            }

            return new StoneDBCompositeDataType(type, size);
        }

        @Override
        public String toString() {
            switch (getPrimitiveDataType()) {
            case INT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("BIGINT", "INT8");
                case 4:
                    return Randomly.fromOptions("INTEGER", "INT", "INT4", "SIGNED");
                case 2:
                    return Randomly.fromOptions("SMALLINT", "INT2");
                case 1:
                    return Randomly.fromOptions("TINYINT", "INT1");
                default:
                    throw new AssertionError(size);
                }
            case VARCHAR:
                return "VARCHAR";
            case FLOAT:
                switch (size) {
                case 8:
                    return Randomly.fromOptions("DOUBLE");
                case 4:
                    return Randomly.fromOptions("REAL", "FLOAT4");
                default:
                    throw new AssertionError(size);
                }
            case BOOLEAN:
                return Randomly.fromOptions("BOOLEAN", "BOOL");
            case NULL:
                return Randomly.fromOptions("NULL");
            default:
                throw new AssertionError(getPrimitiveDataType());
            }
        }

    }

    public static class StoneDBColumn extends AbstractTableColumn<StoneDBTable, StoneDBCompositeDataType> {

        private final boolean isPrimaryKey;
        private final boolean isNullable;

        public StoneDBColumn(String name, StoneDBCompositeDataType columnType, boolean isPrimaryKey, boolean isNullable) {
            super(name, null, columnType);
            this.isPrimaryKey = isPrimaryKey;
            this.isNullable = isNullable;
        }

        public boolean isPrimaryKey() {
            return isPrimaryKey;
        }

        public boolean isNullable() {
            return isNullable;
        }

    }

    public static class StoneDBTables extends AbstractTables<StoneDBTable, StoneDBColumn> {

        public StoneDBTables(List<StoneDBTable> tables) {
            super(tables);
        }

    }

    public StoneDBSchema(List<StoneDBTable> databaseTables) {
        super(databaseTables);
    }

    public StoneDBTables getRandomTableNonEmptyTables() {
        return new StoneDBTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

    private static StoneDBCompositeDataType getColumnType(String typeString) {
        StoneDBDataType primitiveType;
        int size = -1;
        if (typeString.startsWith("DECIMAL")) { // Ugly hack
            return new StoneDBCompositeDataType(StoneDBDataType.FLOAT, 8);
        }
        switch (typeString) {
        case "INTEGER":
            primitiveType = StoneDBDataType.INT;
            size = 4;
            break;
        case "SMALLINT":
            primitiveType = StoneDBDataType.INT;
            size = 2;
            break;
        case "BIGINT":
        case "HUGEINT": // TODO: 16-bit int
            primitiveType = StoneDBDataType.INT;
            size = 8;
            break;
        case "TINYINT":
            primitiveType = StoneDBDataType.INT;
            size = 1;
            break;
        case "VARCHAR":
            primitiveType = StoneDBDataType.VARCHAR;
            break;
        case "FLOAT":
            primitiveType = StoneDBDataType.FLOAT;
            size = 4;
            break;
        case "DOUBLE":
            primitiveType = StoneDBDataType.FLOAT;
            size = 8;
            break;
        case "BOOLEAN":
            primitiveType = StoneDBDataType.BOOLEAN;
            break;
        case "NULL":
            primitiveType = StoneDBDataType.NULL;
            break;
        case "INTERVAL":
            throw new IgnoreMeException();
        // TODO: caused when a view contains a computation like ((TIMESTAMP '1970-01-05 11:26:57')-(TIMESTAMP
        // '1969-12-29 06:50:27'))
        default:
            throw new AssertionError(typeString);
        }
        return new StoneDBCompositeDataType(primitiveType, size);
    }

    public static class StoneDBTable extends AbstractRelationalTable<StoneDBColumn, TableIndex, StoneDBGlobalState> {

        public StoneDBTable(String tableName, List<StoneDBColumn> columns, boolean isView) {
            super(tableName, columns, Collections.emptyList(), isView);
        }

    }

    public static StoneDBSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        List<StoneDBTable> databaseTables = new ArrayList<>();
        List<String> tableNames = getTableNames(con);
        for (String tableName : tableNames) {
            if (DBMSCommon.matchesIndexName(tableName)) {
                continue; // TODO: unexpected?
            }
            List<StoneDBColumn> databaseColumns = getTableColumns(con, tableName);
            boolean isView = tableName.startsWith("v");
            StoneDBTable t = new StoneDBTable(tableName, databaseColumns, isView);
            for (StoneDBColumn c : databaseColumns) {
                c.setTable(t);
            }
            databaseTables.add(t);

        }
        return new StoneDBSchema(databaseTables);
    }

    private static List<String> getTableNames(SQLConnection con) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT * FROM sqlite_master WHERE type='table' or type='view'")) {
                while (rs.next()) {
                    tableNames.add(rs.getString("name"));
                }
            }
        }
        return tableNames;
    }

    private static List<StoneDBColumn> getTableColumns(SQLConnection con, String tableName) throws SQLException {
        List<StoneDBColumn> columns = new ArrayList<>();
        try (Statement s = con.createStatement()) {
            try (ResultSet rs = s.executeQuery(String.format("SELECT * FROM pragma_table_info('%s');", tableName))) {
                while (rs.next()) {
                    String columnName = rs.getString("name");
                    String dataType = rs.getString("type");
                    boolean isNullable = rs.getString("notnull").contentEquals("false");
                    boolean isPrimaryKey = rs.getString("pk").contains("true");
                    StoneDBColumn c = new StoneDBColumn(columnName, getColumnType(dataType), isPrimaryKey, isNullable);
                    columns.add(c);
                }
            }
        }
        if (columns.stream().noneMatch(c -> c.isPrimaryKey())) {
            // https://github.com/cwida/duckdb/issues/589
            // https://github.com/cwida/duckdb/issues/588
            // TODO: implement an option to enable/disable rowids
            columns.add(new StoneDBColumn("rowid", new StoneDBCompositeDataType(StoneDBDataType.INT, 4), false, false));
        }
        return columns;
    }

}
