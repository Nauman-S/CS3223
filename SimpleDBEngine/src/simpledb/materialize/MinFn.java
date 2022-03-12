package simpledb.materialize;

import simpledb.query.*;

/**
 * The <i>min</i> aggregation function.
 * 
 * @author Edward Sciore
 */
public class MinFn extends AggregationFn {
	private Constant val;

	public MinFn(String fldname) {
		super(fldname, AggregateType.MIN);
	}

	public void processFirst(Scan s) {
		val = s.getVal(fldname);
	}

	public void processNext(Scan s) {
		Constant newval = s.getVal(fldname);
		if (newval.compareTo(val) < 0)
			val = newval;
	}

	public String fieldName() {
		return "minof" + fldname;
	}

	public Constant value() {
		return val;
	}
}
