package simpledb.materialize;

import java.util.HashMap;
import java.util.Map;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class SingleRecordScan implements Scan {
	private Map<String, Constant> singleRecordfields;

	public SingleRecordScan(Scan src, Schema schema) {
		this.singleRecordfields = new HashMap<>();
		for (String fldname : schema.fields()) {
			singleRecordfields.put(fldname, src.getVal(fldname));
		}
	}

	@Override
	public void beforeFirst() {
	}

	@Override
	public boolean next() {
		return false;
	}

	@Override
	public int getInt(String fldname) {
		return this.singleRecordfields.get(fldname).asInt();
	}

	@Override
	public String getString(String fldname) {
		return this.singleRecordfields.get(fldname).asString();
	}

	@Override
	public Constant getVal(String fldname) {
		return this.singleRecordfields.get(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return this.singleRecordfields.containsKey(fldname);
	}

	@Override
	public void close() {
	}

}
