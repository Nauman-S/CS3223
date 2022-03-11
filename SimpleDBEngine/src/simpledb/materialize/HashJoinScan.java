package simpledb.materialize;

import java.util.HashMap;
import java.util.List;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class HashJoinScan implements Scan {
	private Transaction tx;
	private List<UpdateScan> lhs, rhs;
	private String joinfield1, joinfield2;
	private int currentPartition;
	private UpdateScan currentScanLhs;
	private UpdateScan currentScanRhs;
	private HashMap<Constant, UpdateScan> currentHashMap;
	private Schema sch;

	public HashJoinScan(Transaction tx, List<UpdateScan> lhs, List<UpdateScan> rhs, String joinfield1,
			String joinfield2, Schema sch) {
		this.tx = tx;
		this.joinfield1 = joinfield1;
		this.joinfield2 = joinfield2;
		this.lhs = lhs;
		this.rhs = rhs;
		this.currentPartition = -1;
		this.sch = sch;
		this.currentHashMap = new HashMap<>();
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		nextPartition();
	}

	public void buildHashTable() {
		currentHashMap.clear();
		currentScanLhs.beforeFirst();
		while (currentScanLhs.next()) {
			Constant constant = currentScanLhs.getVal(joinfield1);
			UpdateScan newUpdateScan = new TableScan(tx, "temp" + constant.toString(), new Layout(sch));
			copy(currentScanLhs, newUpdateScan);
			currentHashMap.put(constant, newUpdateScan);
		}
	}

	@Override
	public boolean next() {
		while (currentScanRhs.next()) {
			Constant constant = currentScanRhs.getVal(joinfield2);
			if (currentHashMap.containsKey(constant)) {
				currentScanLhs = currentHashMap.get(constant);
				return true;
			}
		}
		if (nextPartition()) {
			next();
		}

		return false;
	}

	private boolean nextPartition() {
		currentPartition++;
		if (currentPartition >= lhs.size()) {
			return false;
		}
		currentScanLhs = lhs.get(currentPartition);
		currentScanRhs = rhs.get(currentPartition);
		buildHashTable();
		currentScanRhs.beforeFirst();
		return true;
	}

	@Override
	public int getInt(String fldname) {
		if (currentScanRhs.hasField(fldname))
			return currentScanRhs.getInt(fldname);
		else
			return currentScanLhs.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		if (currentScanRhs.hasField(fldname))
			return currentScanRhs.getString(fldname);
		else
			return currentScanLhs.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		if (currentScanRhs.hasField(fldname))
			return currentScanRhs.getVal(fldname);
		else
			return currentScanLhs.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return currentScanRhs.hasField(fldname) || currentScanRhs.hasField(fldname);
	}

	@Override
	public void close() {
		currentScanRhs.close();
		currentScanLhs.close();
		currentPartition = Integer.MAX_VALUE;
	}

	private boolean copy(Scan src, UpdateScan dest) {
		dest.insert();
		for (String fldname : sch.fields())
			dest.setVal(fldname, src.getVal(fldname));
		return src.next();
	}

}
