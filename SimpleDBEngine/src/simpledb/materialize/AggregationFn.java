package simpledb.materialize;

import simpledb.query.AggregateType;
import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The interface implemented by aggregation functions. Aggregation functions are
 * used by the <i>groupby</i> operator.
 *
 * @author Edward Sciore
 */
public abstract class AggregationFn {

    /**
     * The Aggregate function Type.
     */
    protected AggregateType aggtype;

    /**
     * The fieldname aggregated.
     */
    protected String fldname;

    /**
     * Instantiates a new Aggregation fn.
     *
     * @param fldname the fldname
     * @param aggtype the aggtype
     */
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

    /**
     * Gets field name.
     *
     * @return the name
     */
    public String getfldName() {
        return this.fldname;
    }

    /**
     * Gets aggregate type.
     *
     * @return the agg type
     */
    public AggregateType getAggType() {
        return this.aggtype;
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", aggtype, fldname);
    }

}
