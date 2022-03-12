package simpledb.materialize;

import simpledb.query.*;

/**
 * The interface implemented by aggregation functions. Aggregation functions are
 * used by the <i>groupby</i> operator.
 * 
 * @author Edward Sciore
 */
public abstract class AggregationFn {

	protected AggregateType aggtype;

	protected String fldname;

	public AggregationFn(String fldname, AggregateType aggtype) {
		this.fldname = fldname;
		this.aggtype = aggtype;
	}

	/**
	 * Use the current record of the specified scan to be the first record in the
	 * group.
	 * 
	 * @param s the scan to aggregate over.
	 */
	public abstract void processFirst(Scan s);

	/**
	 * Use the current record of the specified scan to be the next record in the
	 * group.
	 * 
	 * @param s the scan to aggregate over.
	 */
	public abstract void processNext(Scan s);

	/**
	 * Return the name of the new aggregation field.
	 * 
	 * @return the name of the new aggregation field
	 */
	public abstract String fieldName();

	/**
	 * Return the computed aggregation value.
	 * 
	 * @return the computed aggregation value
	 */
	public abstract Constant value();

	public String getfldName() {
		return this.fldname;
	}

	public AggregateType getAggType() {
		return this.aggtype;
	}

}
