package simpledb.materialize;

import java.util.ArrayList;
import java.util.List;

import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class HashJoinPlan implements Plan {
	private Transaction tx;
	private int bufferSize;
	private Plan p1, p2;
	private String joinfield1;
	private String joinfield2;
	private Schema sch = new Schema();

	public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String joinfield1, String joinfield2) {
		this.tx = tx;
		this.bufferSize = tx.availableBuffs();
		this.p1 = p1;
		this.p2 = p2;
		this.joinfield1 = joinfield1;
		this.joinfield2 = joinfield2;
		sch.addAll(p1.schema());
		sch.addAll(p2.schema());
	}

	@Override
	public Scan open() {
		Scan s1 = p1.open();
		List<UpdateScan> partitions1 = splitIntoPartitions(s1, joinfield1);
		Scan s2 = p2.open();
		List<UpdateScan> partitions2 = splitIntoPartitions(s2, joinfield2);

		return new HashJoinScan(tx, partitions1, partitions2, joinfield1, joinfield2, sch);
	}

	@Override
	public int blocksAccessed() {
		// TODO Auto-generated method stub
		return 0;
	}

	public List<UpdateScan> splitIntoPartitions(Scan s, String joinfield) {
		List<UpdateScan> updateScans = new ArrayList<>();
		for (int i = 0; i < bufferSize - 1; i++) {
			TableScan tableScan = new TableScan(tx, "temp" + i, new Layout(sch));
			tableScan.beforeFirst();
			updateScans.add(tableScan);
		}
		s.beforeFirst();
		while (s.next()) {
			int index = graceHash(s.getVal(joinfield));
			UpdateScan currentScan = updateScans.get(index);
			copy(s, currentScan);
			
		}
		s.close();
		updateScans.forEach(updateScan ->updateScan.close());
		

		return updateScans;
	}

	public int graceHash(Constant val) {

		return val.hashCode() % (bufferSize - 1);
	}

	@Override
	public int recordsOutput() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int distinctValues(String fldname) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Schema schema() {
		return sch;
	}

	private void copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : sch.fields())
			if (src.hasField(fldname)) {
				dest.setVal(fldname, src.getVal(fldname));
			}
	}
}
