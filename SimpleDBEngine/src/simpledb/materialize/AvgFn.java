package simpledb.materialize;

import simpledb.query.AggregateType;
import simpledb.query.Constant;
import simpledb.query.Scan;

/**
 * The <i>avg</i> aggregate function.
 *
 * @author Edward Sciore
 */
public class AvgFn extends AggregationFn {
    private int sum;
    private int count;

    /**
     * Instantiates a new AVG aggregate function.
     *
     * @param fldname the fldname
     */
    public AvgFn(String fldname) {
        super(fldname, AggregateType.AVG);
    }

    public void processFirst(Scan s) {
        sum = s.getVal(fldname).asInt();
        count = 1;
    }

    public void processNext(Scan s) {
        sum += s.getVal(fldname).asInt();
        count++;
    }

    public String fieldName() {
        return "avgof" + fldname;
    }

    public Constant value() {
        return new Constant(sum / count);
    }
}
