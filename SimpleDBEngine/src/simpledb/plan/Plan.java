package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.*;

/**
 * The abstract class extended by each query plan. There is a Plan class for
 * each relational algebra operator.
 * 
 * @author Edward Sciore
 *
 */
public abstract class Plan {

	/**
	 * Opens a scan corresponding to this plan. The scan will be positioned before
	 * its first record.
	 * 
	 * @return a scan
	 */
	abstract public Scan open();

	/**
	 * Returns an estimate of the number of block accesses that will occur when the
	 * scan is read to completion.
	 * 
	 * @return the estimated number of block accesses
	 */
	abstract public int blocksAccessed();

	/**
	 * Returns an estimate of the number of records in the query's output table.
	 * 
	 * @return the estimated number of output records
	 */
	abstract public int recordsOutput();

	/**
	 * Returns an estimate of the number of distinct values for the specified field
	 * in the query's output table.
	 * 
	 * @param fldname the name of a field
	 * @return the estimated number of distinct field values in the output
	 */
	abstract public int distinctValues(String fldname);

	/**
	 * Returns the schema of the query.
	 * 
	 * @return the query's schema
	 */
	abstract public Schema schema();

	/**
	 * Returns the formatted string representation of the current plan.
	 * 
	 * @return the query's formatted string representation
	 */
	abstract public String format(int indent);

	@Override
	public String toString() {
		return format(0);
	}
}
