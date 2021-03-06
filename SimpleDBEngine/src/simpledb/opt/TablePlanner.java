package simpledb.opt;

import simpledb.index.planner.IndexJoinPlan;
import simpledb.index.planner.IndexSelectPlan;
import simpledb.materialize.HashJoinPlan;
import simpledb.materialize.MergeJoinPlan;
import simpledb.metadata.IndexInfo;
import simpledb.metadata.MetadataMgr;
import simpledb.multibuffer.MultibufferProductPlan;
import simpledb.plan.NestedLoopJoinPlan;
import simpledb.plan.Plan;
import simpledb.plan.SelectPlan;
import simpledb.plan.TablePlan;
import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.query.Term;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.Map;

/**
 * This class contains methods for planning a single table.
 *
 * @author Edward Sciore
 */
class TablePlanner {
    private TablePlan myplan;
    private Predicate mypred;
    private Schema myschema;
    private Map<String, IndexInfo> indexes;
    private Transaction tx;

    /**
     * Creates a new table planner. The specified predicate applies to the entire
     * query. The table planner is responsible for determining which portion of the
     * predicate is useful to the table, and when indexes are useful.
     *
     * @param tblname the name of the table
     * @param mypred  the query predicate
     * @param tx      the calling transaction
     * @param mdm     the metadata manager
     */
    public TablePlanner(String tblname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
        this.mypred = mypred;
        this.tx = tx;
        myplan = new TablePlan(tx, tblname, mdm);
        myschema = myplan.schema();
        indexes = mdm.getIndexInfo(tblname, tx);
    }

    /**
     * Constructs a select plan for the table. The plan will use an indexselect, if
     * possible.
     *
     * @return a select plan for the table.
     */
    public Plan makeSelectPlan() {
        Plan p = makeIndexSelect();
        if (p == null) p = myplan;
        return addSelectPred(p);
    }

    /**
     * Constructs a join plan of the specified plan and the table. The plan will use
     * a nested loop join by if possible, else uses product join. The method returns null
     * if no join is possible.
     *
     * @param current the specified plan
     * @return a join plan of the plan and this table
     */
    public Plan makeJoinPlan(Plan current) {
        Schema currsch = current.schema();
        Predicate joinpred = mypred.joinSubPred(myschema, currsch);
        if (joinpred == null) return null;
//		Plan p = makeIndexJoin(current, currsch);
//		Plan p = makeMergeJoin(current, currsch, joinpred);
        Plan p = makeNestedLoopJoin(current, currsch);
//		Plan p = makeHashJoin(current, currsch);

//		Plan p = chooseBestJoinPlan(current, currsch, joinpred);
        if (p == null) p = makeProductJoin(current, currsch);
        return p;
    }

    private Plan chooseBestJoinPlan(Plan current, Schema currsch, Predicate joinpred) {
        Plan p = makeNestedLoopJoin(current, currsch);

        if (p == null) {
            // If nested loop join cant be made, nothing can.
            return null;
        }

        Plan indexJoinPlan = makeIndexJoin(current, currsch);
        int bestCost = p.blocksAccessed();

        if (indexJoinPlan != null) {
            int indexCost = indexJoinPlan.blocksAccessed();

            if (bestCost > indexCost) {
                p = indexJoinPlan;
                bestCost = indexCost;
            }
        }
        if (!joinpred.getTerms().isEmpty() && joinpred.getTerms().get(0).getOperator().isEquals()) {
            // Equality join predicate
            Plan mergeJoinPlan = makeMergeJoin(current, currsch, joinpred);
            int mergeCost = mergeJoinPlan.blocksAccessed();

            Plan hashJoinPlan = makeHashJoin(current, currsch);
            int hashCost = hashJoinPlan.blocksAccessed();

            if (bestCost > mergeCost) {
                p = mergeJoinPlan;
                bestCost = mergeCost;
            }

            if (bestCost > hashCost) {
                p = hashJoinPlan;
            }
        }
        return p;
    }

    /**
     * Constructs a product plan of the specified plan and this table.
     *
     * @param current the specified plan
     * @return a product plan of the specified plan and this table
     */
    public Plan makeProductPlan(Plan current) {
        Plan p = addSelectPred(myplan);
        return new MultibufferProductPlan(tx, current, p);
    }

    private Plan makeIndexSelect() {
        for (String fldname : indexes.keySet()) {
            Constant val = mypred.equatesWithConstant(fldname);
            if (val != null) {
                IndexInfo ii = indexes.get(fldname);
                System.out.println("index on " + fldname + " used");
                return new IndexSelectPlan(myplan, ii, val);
            }
        }
        return null;
    }

    private Plan makeHashJoin(Plan current, Schema currsch) {
        Predicate joinpred = mypred.joinSubPred(currsch, myschema);
        if (joinpred == null) return null;

        Term term = joinpred.getTerms().get(0);

        String fieldName1 = term.getLhs().asFieldName();
        String fieldName2 = term.getRhs().asFieldName();

        if (!myplan.schema().hasField(fieldName1)) {
            String temp = fieldName1;
            fieldName1 = fieldName2;
            fieldName2 = temp;
        }
        Plan p = new HashJoinPlan(tx, myplan, current, fieldName1, fieldName2);
        p = addSelectPred(p);
        return addJoinPred(p, currsch);
    }

    private Plan makeIndexJoin(Plan current, Schema currsch) {
        for (String fldname : indexes.keySet()) {
            String outerfield = mypred.equatesWithField(fldname);
            if (outerfield != null && currsch.hasField(outerfield)) {
                IndexInfo ii = indexes.get(fldname);
                Plan p = new IndexJoinPlan(current, myplan, ii, outerfield);
                p = addSelectPred(p);
                return addJoinPred(p, currsch);
            }
        }
        return null;
    }

    private Plan makeMergeJoin(Plan current, Schema currsch, Predicate pred) {
        if (pred.getTerms().isEmpty()) {
            return null;
        }
        Term term = pred.getTerms().get(0);
        String fieldName1 = term.getLhs().asFieldName();
        String fieldName2 = term.getRhs().asFieldName();

        if (!myplan.schema().hasField(fieldName1)) {
            String temp = fieldName1;
            fieldName1 = fieldName2;
            fieldName2 = temp;
        }

        Plan p = new MergeJoinPlan(tx, myplan, current, fieldName1, fieldName2);
        p = addSelectPred(p);
        return addJoinPred(p, currsch);
    }

    private Plan makeNestedLoopJoin(Plan current, Schema currsch) {
        for (String fldname : myschema.fields()) {
            String outerfield = mypred.equatesWithField(fldname);
            if (outerfield != null && currsch.hasField(outerfield)) {
                Plan p = new NestedLoopJoinPlan(current, myplan, outerfield, fldname);
                p = addSelectPred(p);
                return addJoinPred(p, currsch);
            }
        }
        return null;
    }

    private Plan makeProductJoin(Plan current, Schema currsch) {
        Plan p = makeProductPlan(current);
        return addJoinPred(p, currsch);
    }

    private Plan addSelectPred(Plan p) {
        Predicate selectpred = mypred.selectSubPred(myschema);
        if (selectpred != null) return new SelectPlan(p, selectpred);
        else return p;
    }

    private Plan addJoinPred(Plan p, Schema currsch) {
        Predicate joinpred = mypred.joinSubPred(currsch, myschema);
        if (joinpred != null) return new SelectPlan(p, joinpred);
        else return p;
    }
}
