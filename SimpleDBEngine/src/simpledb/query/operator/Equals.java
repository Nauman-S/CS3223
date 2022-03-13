package simpledb.query.operator;

/**
 * A class that represents the equals operator.
 */

public class Equals extends Operator {
	public static final String OPERATOR_STRING = "=";

	Equals() {
		super();
	}

	@Override
	public <T extends Comparable<T>> boolean apply(T lhs, T rhs) {
		return lhs.equals(rhs);
	}

	@Override
	public boolean isEquals() {
		return true;
	}

	@Override
	public String toString() {
		return OPERATOR_STRING;
	}
}