package simpledb.query;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.AvgFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.materialize.MinFn;
import simpledb.materialize.SumFn;

public class AggregateField extends Field {

	public enum Type {
		MAX, MIN, SUM, COUNT, AVG
	}

	private AggregationFn aggregateFn;

	public AggregateField(String fldname, Type type) {
		super(fldname);
		this.aggregateFn = getAggregateFunction(type);
	}

	private AggregationFn getAggregateFunction(Type type) {
		switch (type) {
		case MAX:
			return new MaxFn(fldname);
		case MIN:
			return new MinFn(fldname);
		case SUM:
			return new SumFn(fldname);
		case COUNT:
			return new CountFn(fldname);
		case AVG:
			return new AvgFn(fldname);
		}

		return null;
	}

	@Override
	public boolean isAggregated() {
		return true;
	}

	public AggregationFn getAggregationFunction() {
		return aggregateFn;
	}

}
