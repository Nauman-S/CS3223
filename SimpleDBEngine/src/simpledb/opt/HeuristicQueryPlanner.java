package simpledb.opt;

import simpledb.materialize.GroupByPlan;
import simpledb.materialize.SortPlan;
import simpledb.metadata.MetadataMgr;
import simpledb.parse.QueryData;
import simpledb.plan.DistinctPlan;
import simpledb.plan.Plan;
import simpledb.plan.Planner;
import simpledb.plan.ProjectPlan;
import simpledb.plan.QueryPlanner;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 *
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
    private Collection<TablePlanner> tableplanners = new ArrayList<>();
    private MetadataMgr mdm;

    /**
     * Instantiates a new Heuristic query planner.
     *
     * @param mdm the metadata manager
     */
    public HeuristicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    /**
     * Creates an optimized left-deep query plan using the following heuristics. H1.
     * Choose the smallest table (considering selection predicates) to be first in
     * the join order. H2. Add the table to the join order which results in the
     * smallest output.
     *
     * @param data the query data
     * @param tx the transaction
     */
    public Plan createPlan(QueryData data, Transaction tx) {

        // Step 1: Create a TablePlanner object for each mentioned table
        for (String tblname : data.tables()) {
            TablePlanner tp = new TablePlanner(tblname, data.pred(), tx, mdm);
            tableplanners.add(tp);
        }

        // Step 2: Choose the lowest-size plan to begin the join order
        Plan currentplan = getLowestSelectPlan();

        // Step 3: Repeatedly add a plan to the join order
        while (!tableplanners.isEmpty()) {
            Plan p = getLowestJoinPlan(currentplan);
            if (p != null) currentplan = p;
            else // no applicable join
                currentplan = getLowestProductPlan(currentplan);
        }

        // Step 4: Project on the field names
        if (data.isAggregate()) {
            currentplan = getSortPlan(tx, currentplan, data);
            currentplan = new GroupByPlan(tx, currentplan, data.groupByList(), data.aggfns());
            List<String> fieldlist = data.fields();
            data.aggfns().forEach(aggfn -> fieldlist.add(aggfn.fieldName()));
            currentplan = getProjectPlan(tx, currentplan, data, fieldlist);
        } else {
            currentplan = getProjectPlan(tx, currentplan, data, data.fields());
            currentplan = getSortPlan(tx, currentplan, data);
        }

        return currentplan;
    }

    private Plan getProjectPlan(Transaction tx, Plan currentplan, QueryData data,
                                List<String> fields) {
        if (data.isDistinct()) {
            return new DistinctPlan(tx, currentplan, fields);
        }
        return new ProjectPlan(currentplan, fields);
    }

    private Plan getSortPlan(Transaction tx, Plan currentplan, QueryData data) {
        if (data.orderPairs().isEmpty()) return currentplan;
        return new SortPlan(tx, currentplan, data.orderPairs());
    }

    private Plan getLowestSelectPlan() {
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tableplanners) {
            Plan plan = tp.makeSelectPlan();
            if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
                besttp = tp;
                bestplan = plan;
            }
        }
        tableplanners.remove(besttp);
        return bestplan;
    }

    private Plan getLowestJoinPlan(Plan current) {
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tableplanners) {
            Plan plan = tp.makeJoinPlan(current);
            if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
                besttp = tp;
                bestplan = plan;
            }
        }
        if (bestplan != null) tableplanners.remove(besttp);
        return bestplan;
    }

    private Plan getLowestProductPlan(Plan current) {
        TablePlanner besttp = null;
        Plan bestplan = null;
        for (TablePlanner tp : tableplanners) {
            Plan plan = tp.makeProductPlan(current);
            if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
                besttp = tp;
                bestplan = plan;
            }
        }
        tableplanners.remove(besttp);
        return bestplan;
    }

    /**
     * Sets planner.
     *
     * @param p the p
     */
    public void setPlanner(Planner p) {
        // for use in planning views, which
        // for simplicity this code doesn't do.
    }
}
