package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>avg</i> aggregate function.
 * 
 * @author Edward Sciore
 */
public class AvgFn extends AggregationFn {
	private int sum;
	private int count;

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
