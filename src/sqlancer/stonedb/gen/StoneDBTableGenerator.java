package sqlancer.stonedb.gen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import sqlancer.Randomly;
import sqlancer.common.DBMSCommon;
import sqlancer.common.ast.newast.Node;
import sqlancer.common.gen.UntypedExpressionGenerator;
import sqlancer.common.query.ExpectedErrors;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.stonedb.StoneDBErrors;
import sqlancer.stonedb.StoneDBGlobalState;
import sqlancer.stonedb.StoneDBSchema;
import sqlancer.stonedb.StoneDBSchema.StoneDBColumn;
import sqlancer.stonedb.StoneDBSchema.StoneDBCompositeDataType;
import sqlancer.stonedb.StoneDBSchema.StoneDBDataType;
import sqlancer.stonedb.StoneDBToStringVisitor;
import sqlancer.stonedb.ast.StoneDBExpression;

public class StoneDBTableGenerator {
	
	private StringBuilder sb = new StringBuilder();
	private final StoneDBGlobalState globalState;
	private final String tableName;
	private final Randomly r;
	private final StoneDBSchema schema;
	private final boolean allowPrimaryKey;
	private final List<String> columns = new ArrayList<>();
	private int columnId;
	private boolean tableHasNullableColumn;
	private int keysSpecified;
	private boolean setPrimaryKey;
	
	public StoneDBTableGenerator(StoneDBGlobalState globalState, String tableName) {
		this.tableName = tableName;
		this.r = globalState.getRandomly();
		this.schema = globalState.getSchema();
		this.allowPrimaryKey = Randomly.getBoolean();
		this.globalState = globalState;
	}
	
	public SQLQueryAdapter generate(StoneDBGlobalState globalState) {
		ExpectedErrors errors = new ExpectedErrors();
        
        sb.append("CREATE TABLE");
        if (Randomly.getBoolean()) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ");
        sb.append(this.tableName);
        
        if (Randomly.getBoolean() && !schema.getDatabaseTables().isEmpty()) {
        	sb.append(" LIKE ");
        	sb.append(schema.getRandomTable().getName());
        	return new SQLQueryAdapter(sb.toString(), true);
        }
        
        sb.append("(");
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            appendColumn();
        }
        sb.append(") ");
        List<StoneDBColumn> columns = getNewColumns();
        UntypedExpressionGenerator<Node<StoneDBExpression>, StoneDBColumn> gen = new StoneDBExpressionGenerator(
                globalState).setColumns(columns);
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i).getName());
            sb.append(" ");
            sb.append(columns.get(i).getType());
            if (globalState.getDbmsSpecificOptions().testCollate && Randomly.getBooleanWithRatherLowProbability()
                    && columns.get(i).getType().getPrimitiveDataType() == StoneDBDataType.VARCHAR) {
                sb.append(" COLLATE ");
                sb.append(getRandomCollate());
            }
            if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" UNIQUE");
            }
            if (globalState.getDbmsSpecificOptions().testNotNullConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" NOT NULL");
            }
            if (globalState.getDbmsSpecificOptions().testCheckConstraints
                    && Randomly.getBooleanWithRatherLowProbability()) {
                sb.append(" CHECK(");
                sb.append(StoneDBToStringVisitor.asString(gen.generateExpression()));
                StoneDBErrors.addExpressionErrors(errors);
                sb.append(")");
            }
            if (Randomly.getBoolean() && globalState.getDbmsSpecificOptions().testDefaultValues) {
                sb.append(" DEFAULT(");
                sb.append(StoneDBToStringVisitor.asString(gen.generateConstant()));
                sb.append(")");
            }
        }
        if (globalState.getDbmsSpecificOptions().testIndexes && Randomly.getBoolean()) {
            errors.add("Invalid type for index");
            List<StoneDBColumn> primaryKeyColumns = Randomly.nonEmptySubset(columns);
            sb.append(", PRIMARY KEY(");
            sb.append(primaryKeyColumns.stream().map(c -> c.getName()).collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append(")");
        return new SQLQueryAdapter(sb.toString(), errors, true);
	}
    
    private void appendColumn() {
        String columnName = DBMSCommon.createColumnName(columnId);
        columns.add(columnName);
        sb.append(columnName);
        appendColumnDefinition();
        columnId++;
    }
    
    private void addCommonErrors(ExpectedErrors list) {
        list.add("The storage engine for the table doesn't support");
        list.add("doesn't have this option");
        list.add("must include all columns");
        list.add("not allowed type for this type of partitioning");
        list.add("doesn't support BLOB/TEXT columns");
        list.add("A BLOB field is not allowed in partition function");
        list.add("Too many keys specified; max 1 keys allowed");
        list.add("The total length of the partitioning fields is too large");
        list.add("Got error -1 - 'Unknown error -1' from storage engine");
    }

    private enum PartitionOptions {
        HASH, KEY
    }

    private enum ColumnOptions {
        NULL_OR_NOT_NULL, UNIQUE, COMMENT, COLUMN_FORMAT, STORAGE, PRIMARY_KEY
    }
    
    private void appendColumnDefinition() {
        sb.append(" ");
        StoneDBDataType randomType = StoneDBDataType.getRandomWithoutNull();
        boolean isTextType = randomType == StoneDBDataType.VARCHAR;
        boolean isBooleanType = randomType == StoneDBDataType.BOOLEAN;
        appendTypeString(randomType);
        sb.append(" ");
        boolean isNull = false;
        boolean columnHasPrimaryKey = false;

        List<ColumnOptions> columnOptions = Randomly.subset(ColumnOptions.values());
        if (!columnOptions.contains(ColumnOptions.NULL_OR_NOT_NULL)) {
            this.tableHasNullableColumn = true;
        }
        if (isTextType || isBooleanType) {
            columnOptions.remove(ColumnOptions.PRIMARY_KEY);
            columnOptions.remove(ColumnOptions.UNIQUE);
        }
        for (ColumnOptions o : columnOptions) {
            sb.append(" ");
            switch (o) {
            case NULL_OR_NOT_NULL:
                // PRIMARY KEYs cannot be NULL
                if (!columnHasPrimaryKey) {
                    if (Randomly.getBoolean()) {
                        sb.append("NULL");
                    }
                    tableHasNullableColumn = true;
                    isNull = true;
                } else {
                    sb.append("NOT NULL");
                }
                break;
            case UNIQUE:
                sb.append("UNIQUE");
                keysSpecified++;
                if (Randomly.getBoolean()) {
                    sb.append(" KEY");
                }
                break;
            case COMMENT:
                // TODO: generate randomly
                sb.append(String.format("COMMENT '%s' ", "asdf"));
                break;
            case COLUMN_FORMAT:
                sb.append("COLUMN_FORMAT ");
                sb.append(Randomly.fromOptions("FIXED", "DYNAMIC", "DEFAULT"));
                break;
            case STORAGE:
                sb.append("STORAGE ");
                sb.append(Randomly.fromOptions("DISK", "MEMORY"));
                break;
            case PRIMARY_KEY:
                // PRIMARY KEYs cannot be NULL
                if (allowPrimaryKey && !setPrimaryKey && !isNull) {
                    sb.append("PRIMARY KEY");
                    setPrimaryKey = true;
                    columnHasPrimaryKey = true;
                }
                break;
            default:
                throw new AssertionError();
            }
        }

    }
    
    private void appendTypeString(StoneDBDataType randomType) {
        switch (randomType) {
        case DECIMAL:
            sb.append("DECIMAL");
            optionallyAddPrecisionAndScale(sb);
            break;
        case INT:
            sb.append(Randomly.fromOptions("TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT"));
            if (Randomly.getBoolean()) {
                sb.append("(");
                sb.append(Randomly.getNotCachedInteger(0, 255)); // Display width out of range for column 'c0' (max =
                                                                 // 255)
                sb.append(")");
            }
            break;
        case VARCHAR:
            sb.append(Randomly.fromOptions("VARCHAR(500)", "TINYTEXT", "TEXT", "MEDIUMTEXT", "LONGTEXT"));
            break;
        case BOOLEAN:
        	sb.append("BOOLEAN");
        	break;
        case FLOAT:
            sb.append("FLOAT");
            optionallyAddPrecisionAndScale(sb);
            break;
        case DOUBLE:
            sb.append(Randomly.fromOptions("DOUBLE", "FLOAT"));
            optionallyAddPrecisionAndScale(sb);
            break;
        default:
            throw new AssertionError();
        }
        
        if (randomType.isNumeric()) {
            if (Randomly.getBoolean() && randomType != StoneDBDataType.INT) {
                sb.append(" UNSIGNED");
            }
            if (Randomly.getBoolean()) {
                sb.append(" ZEROFILL");
            }
        }
    }

    public static void optionallyAddPrecisionAndScale(StringBuilder sb) {
        if (Randomly.getBoolean()) {
            sb.append("(");
            // The maximum number of digits (M) for DECIMAL is 65
            long m = Randomly.getNotCachedInteger(1, 65);
            sb.append(m);
            sb.append(", ");
            // The maximum number of supported decimals (D) is 30
            long nCandidate = Randomly.getNotCachedInteger(1, 30);
            // For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column 'c0').
            long n = Math.min(nCandidate, m);
            sb.append(n);
            sb.append(")");
        }
    }

    public static String getRandomCollate() {
        return Randomly.fromOptions("NOCASE", "NOACCENT", "NOACCENT.NOCASE", "C", "POSIX");
    }

    private static List<StoneDBColumn> getNewColumns() {
        List<StoneDBColumn> columns = new ArrayList<>();
        for (int i = 0; i < Randomly.smallNumber() + 1; i++) {
            String columnName = String.format("c%d", i);
            StoneDBCompositeDataType columnType = StoneDBCompositeDataType.getRandomWithoutNull();
            columns.add(new StoneDBColumn(columnName, columnType, false, false));
        }
        return columns;
    }

}
