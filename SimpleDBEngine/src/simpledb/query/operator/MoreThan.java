package simpledb.query.operator;

/**
 * A class that represents the more than operator.
 */

public class MoreThan extends Operator {
    public static final String OPERATOR_STRING = ">";

    MoreThan() {
        super();
    }

    @Override
    public <T extends Comparable<T>> boolean apply(T lhs, T rhs) {
        return lhs.compareTo(rhs) > 0;
    }

    @Override
    public String toString() {
        return OPERATOR_STRING;
    }
}
