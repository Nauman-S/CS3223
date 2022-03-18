package simpledb.query;

import simpledb.materialize.AggregationFn;
import simpledb.materialize.AvgFn;
import simpledb.materialize.CountFn;
import simpledb.materialize.MaxFn;
import simpledb.materialize.MinFn;
import simpledb.materialize.SumFn;

/**
 * The class Aggregate field.
 */
public class AggregateField extends Field {

    private AggregationFn aggregateFn;

    /**
     * Instantiates a new Aggregate field.
     *
     * @param fldname the fldname
     * @param type    the type
     */
    public AggregateField(String fldname, AggregateType type) {
        super(fldname);
        this.aggregateFn = getAggregateFunction(type);
    }

    private AggregationFn getAggregateFunction(AggregateType type) {
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

    /**
     * Gets aggregation function.
     *
     * @return the aggregation function
     */
    public AggregationFn getAggregationFunction() {
        return aggregateFn;
    }

}
