package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;

/**
 * A Plan class corresponding to the <i>product</i> relational algebra operator
 * that determines the most efficient ordering of its inputs.
 *
 * @author Edward Sciore
 */
public class OptimizedProductPlan extends Plan {
    private Plan bestplan;

    /**
     * Instantiates a new Optimized product plan.
     *
     * @param p1 the p 1
     * @param p2 the p 2
     */
    public OptimizedProductPlan(Plan p1, Plan p2) {
        Plan prod1 = new ProductPlan(p1, p2);
        Plan prod2 = new ProductPlan(p2, p1);
        int b1 = prod1.blocksAccessed();
        int b2 = prod2.blocksAccessed();
        bestplan = (b1 < b2) ? prod1 : prod2;
    }

    public Scan open() {
        return bestplan.open();
    }

    public int blocksAccessed() {
        return bestplan.blocksAccessed();
    }

    public int recordsOutput() {
        return bestplan.recordsOutput();
    }

    public int distinctValues(String fldname) {
        return bestplan.distinctValues(fldname);
    }

    public Schema schema() {
        return bestplan.schema();
    }

    @Override
    public String format(int indent) {
        String indentStr = "\t".repeat(indent);
        StringBuilder sb = new StringBuilder();
        sb.append("product optimised:\n");
        sb.append(String.format("%s{%s}", indentStr, bestplan.format(indent + 1)));
        return sb.toString();
    }

}
