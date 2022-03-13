package simpledb.plan;

import java.util.ArrayList;
import java.util.List;

import simpledb.materialize.MaterializePlan;
import simpledb.materialize.RecordComparator;
import simpledb.materialize.SingleRecordScan;
import simpledb.materialize.TempTable;
import simpledb.query.OrderPair;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class DistinctPlan extends Plan {
	private Transaction tx;
	private Plan p;
	private Schema schema = new Schema();
	private RecordComparator comp;
	private Scan previousScan;

	public DistinctPlan(Transaction tx, Plan p, List<String> fieldlist) {
		this.tx = tx;
		this.p = p;
		for (String fldname : fieldlist)
			schema.add(fldname, p.schema());
		List<OrderPair> sortfields = new ArrayList<>();
		for (String field : fieldlist) {
			sortfields.add(new OrderPair(field));
		}
		comp = new RecordComparator(sortfields);
		previousScan = null;
	}

	@Override
	public Scan open() {
		Scan src = p.open();
		List<TempTable> runs = splitIntoRuns(src);
		src.close();
		while (runs.size() > 1)
			runs = doAMergeIteration(runs);
		return new DistinctScan(runs, comp);
	}

	@Override
	public int blocksAccessed() {
		Plan mp = new MaterializePlan(tx, p); // not opened; just for analysis
		return mp.blocksAccessed();
	}

	@Override
	public int recordsOutput() {
		return p.recordsOutput();
	}

	@Override
	public int distinctValues(String fldname) {
		return p.distinctValues(fldname);
	}

	@Override
	public Schema schema() {
		return schema;
	}

	private List<TempTable> doAMergeIteration(List<TempTable> runs) {
		List<TempTable> result = new ArrayList<>();
		while (runs.size() > 1) {
			TempTable p1 = runs.remove(0);
			TempTable p2 = runs.remove(0);
			result.add(mergeTwoRuns(p1, p2));
		}
		if (runs.size() == 1)
			result.add(runs.get(0));
		return result;
	}

	// upgrade this method
	private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
		Scan src1 = p1.open();
		Scan src2 = p2.open();
		TempTable result = new TempTable(tx, schema);
		UpdateScan dest = result.open();

		boolean hasmore1 = src1.next();
		boolean hasmore2 = src2.next();
		while (hasmore1 && hasmore2) {
			if (comp.compare(src1, src2) < 0) {
				hasmore1 = tryCopy(src1, dest);
			} else {
				hasmore2 = tryCopy(src2, dest);
			}
		}

		if (hasmore1)
			while (hasmore1)
				hasmore1 = tryCopy(src1, dest);
		else
			while (hasmore2)
				hasmore2 = tryCopy(src2, dest);
		src1.close();
		src2.close();
		dest.close();
		return result;
	}

	private boolean tryCopy(Scan src, UpdateScan dest) {
		if (previousScan == null || comp.compare(src, previousScan) != 0) {
			previousScan = new SingleRecordScan(src, schema);
			return copy(src, dest);
		}
		return src.next();
	}

	private List<TempTable> splitIntoRuns(Scan src) {
		List<TempTable> temps = new ArrayList<>();
		src.beforeFirst();
		if (!src.next())
			return temps;
		TempTable currenttemp = new TempTable(tx, schema);
		temps.add(currenttemp);
		UpdateScan currentscan = currenttemp.open();
		while (copy(src, currentscan))
			if (comp.compare(src, currentscan) < 0) {
				// start a new run
				currentscan.close();
				currenttemp = new TempTable(tx, schema);
				temps.add(currenttemp);
				currentscan = (UpdateScan) currenttemp.open();
			}
		currentscan.close();
		return temps;
	}

	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : schema.fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}

	@Override
	public String format(int indent) {
		return null;
	}

}
