package simpledb.query.operator;

/**
 * A class that represents the less than or equals operator.
 */

public class LessThanOrEquals extends Operator {
    public static final String OPERATOR_STRING = "<=";

    LessThanOrEquals() {
        super();
    }

    @Override
    public <T extends Comparable<T>> boolean apply(T lhs, T rhs) {
        return lhs.compareTo(rhs) <= 0;
    }

    @Override
    public String toString() {
        return OPERATOR_STRING;
    }
}
