package simpledb.plan;

import simpledb.query.ProjectScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

import java.util.List;

/**
 * The Plan class corresponding to the <i>project</i> relational algebra
 * operator.
 *
 * @author Edward Sciore
 */
public class ProjectPlan extends Plan {
    private Plan p;
    private Schema schema = new Schema();

    /**
     * Creates a new project node in the query tree, having the specified subquery
     * and field list.
     *
     * @param p         the subquery
     * @param fieldlist the list of fields
     */
    public ProjectPlan(Plan p, List<String> fieldlist) {
        this.p = p;
        for (String fldname : fieldlist)
            schema.add(fldname, p.schema());
    }

    /**
     * Creates a project scan for this query.
     *
     * @see simpledb.plan.Plan#open()
     */
    public Scan open() {
        Scan s = p.open();
        return new ProjectScan(s, schema.fields());
    }

    /**
     * Estimates the number of block accesses in the projection, which is the same
     * as in the underlying query.
     *
     * @see simpledb.plan.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    /**
     * Estimates the number of output records in the projection, which is the same
     * as in the underlying query.
     *
     * @see simpledb.plan.Plan#recordsOutput()
     */
    public int recordsOutput() {
        return p.recordsOutput();
    }

    /**
     * Estimates the number of distinct field values in the projection, which is the
     * same as in the underlying query.
     *
     * @see simpledb.plan.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        return p.distinctValues(fldname);
    }

    /**
     * Returns the schema of the projection, which is taken from the field list.
     *
     * @see simpledb.plan.Plan#schema()
     */
    public Schema schema() {
        return schema;
    }

    @Override
    public String format(int indent) {
        String indentStr = "\t".repeat(indent);
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("project: %s\n", schema.fields()));
        sb.append(String.format("%s{%s}", indentStr, p.format(indent + 1)));
        return sb.toString();
    }
}
