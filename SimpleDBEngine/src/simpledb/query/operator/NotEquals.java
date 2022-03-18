package simpledb.query.operator;

/**
 * A class that represents the not equals operator.
 */

public class NotEquals extends Operator {
    public static final String OPERATOR_STRING_1 = "!=";
    public static final String OPERATOR_STRING_2 = "<>";

    NotEquals() {
        super();
    }

    @Override
    public <T extends Comparable<T>> boolean apply(T lhs, T rhs) {
        return !lhs.equals(rhs);
    }

    @Override
    public String toString() {
        return OPERATOR_STRING_1;
    }
}
