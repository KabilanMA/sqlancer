package sqlancer.stonedb.ast;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import sqlancer.common.ast.newast.Node;

public class StoneDBConstant implements Node<StoneDBExpression> {

    private StoneDBConstant() {
    }

    public static class StoneDBNullConstant extends StoneDBConstant {

        @Override
        public String toString() {
            return "NULL";
        }

    }

    public static class StoneDBIntConstant extends StoneDBConstant {

        private final long value;

        public StoneDBIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

    }

    public static class StoneDBDoubleConstant extends StoneDBConstant {

        private final double value;

        public StoneDBDoubleConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }

    }

    public static class StoneDBTextConstant extends StoneDBConstant {

        private final String value;

        public StoneDBTextConstant(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''") + "'";
        }

    }

    public static class StoneDBBitConstant extends StoneDBConstant {

        private final String value;

        public StoneDBBitConstant(long value) {
            this.value = Long.toBinaryString(value);
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "B'" + value + "'";
        }

    }

    public static class StoneDBDateConstant extends StoneDBConstant {

        public String textRepr;

        public StoneDBDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepr);
        }

    }

    public static class StoneDBTimestampConstant extends StoneDBConstant {

        public String textRepr;

        public StoneDBTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }

    }

    public static class StoneDBBooleanConstant extends StoneDBConstant {

        private final boolean value;

        public StoneDBBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

    }

    public static Node<StoneDBExpression> createStringConstant(String text) {
        return new StoneDBTextConstant(text);
    }

    public static Node<StoneDBExpression> createFloatConstant(double val) {
        return new StoneDBDoubleConstant(val);
    }

    public static Node<StoneDBExpression> createIntConstant(long val) {
        return new StoneDBIntConstant(val);
    }

    public static Node<StoneDBExpression> createNullConstant() {
        return new StoneDBNullConstant();
    }

    public static Node<StoneDBExpression> createBooleanConstant(boolean val) {
        return new StoneDBBooleanConstant(val);
    }

    public static Node<StoneDBExpression> createDateConstant(long integer) {
        return new StoneDBDateConstant(integer);
    }

    public static Node<StoneDBExpression> createTimestampConstant(long integer) {
        return new StoneDBTimestampConstant(integer);
    }

}
