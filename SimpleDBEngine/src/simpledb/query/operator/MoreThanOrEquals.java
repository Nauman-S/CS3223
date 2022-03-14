package simpledb.query.operator;

/**
 * A class that represents the more than or equals operator.
 */

public class MoreThanOrEquals extends Operator {
	public static final String OPERATOR_STRING = ">=";

	MoreThanOrEquals() {
		super();
	}

	@Override
	public <T extends Comparable<T>> boolean apply(T lhs, T rhs) {
		return lhs.compareTo(rhs) >= 0;
	}

	@Override
	public String toString() {
		return OPERATOR_STRING;
	}
}
