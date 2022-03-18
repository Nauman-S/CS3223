package simpledb.materialize;

import simpledb.query.AggregateType;
import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>sum</i> aggregate function.
 *
 * @author Edward Sciore
 */
public class SumFn extends AggregationFn {
    private int sum;

    /**
     * Instantiates a new SUM aggregate function.
     *
     * @param fldname the fldname
     */
    public SumFn(String fldname) {
        super(fldname, AggregateType.SUM);
    }

    public void processFirst(Scan s) {
        sum = s.getVal(fldname).asInt();
    }

    public void processNext(Scan s) {
        sum += s.getVal(fldname).asInt();
    }

    public String fieldName() {
        return "sumof" + fldname;
    }

    public Constant value() {
        return new Constant(sum);
    }
}
