package simpledb.query.operator;

import simpledb.parse.BadSyntaxException;

/**
 * An abstract class that represents an operator.
 */
public abstract class Operator {

	/**
	 * A method that applies the operator on the specified arguments.
	 *
	 * @param <T  extends Comparable<T>>
	 * @param lhs the lhs to compare
	 * @param rhs the rhs to compare
	 * @return true if the relationship holds
	 */
	public abstract <T extends Comparable<T>> boolean apply(T lhs, T rhs);

	public boolean isEquals() {
		return false;
	}

	/**
	 * Creates the operator specified.
	 *
	 * @param operatorstr the string of the operator to create
	 * @return operator if it is valid
	 */
	public static Operator of(String operatorstr) {
		switch (operatorstr) {
		case Equals.OPERATOR_STRING:
			return new Equals();
		case NotEquals.OPERATOR_STRING_1:
		case NotEquals.OPERATOR_STRING_2:
			return new NotEquals();
		case LessThan.OPERATOR_STRING:
			return new LessThan();
		case LessThanOrEquals.OPERATOR_STRING:
			return new LessThanOrEquals();
		case MoreThan.OPERATOR_STRING:
			return new MoreThan();
		case MoreThanOrEquals.OPERATOR_STRING:
			return new MoreThanOrEquals();
		}
		// should never reach this path
		throw new BadSyntaxException();
	}
}