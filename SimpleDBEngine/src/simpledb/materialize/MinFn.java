package simpledb.materialize;

import simpledb.query.AggregateType;
import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>min</i> aggregation function.
 *
 * @author Edward Sciore
 */
public class MinFn extends AggregationFn {
    private Constant val;

    /**
     * Instantiates a new MIN aggregate function.
     *
     * @param fldname the fldname
     */
    public MinFn(String fldname) {
        super(fldname, AggregateType.MIN);
    }

    public void processFirst(Scan s) {
        val = s.getVal(fldname);
    }

    public void processNext(Scan s) {
        Constant newval = s.getVal(fldname);
        if (newval.compareTo(val) < 0) val = newval;
    }

    public String fieldName() {
        return "minof" + fldname;
    }

    public Constant value() {
        return val;
    }
}
