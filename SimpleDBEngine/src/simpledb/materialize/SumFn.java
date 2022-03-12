package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>sum</i> aggregate function.
 * 
 * @author Edward Sciore
 */
public class SumFn extends AggregationFn {
	private int sum;

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
