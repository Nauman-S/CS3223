package simpledb.query;

public class NestedLoopJoinScan implements Scan {
	private Scan lhs, rhs;
	private String joinfield, fieldRhs;

	/**
	 * Creates an nested loop join scan for the specified LHS scan and RHS scan.
	 * 
	 * @param lhs       the LHS scan
	 * @param rhs       the RHS scan
	 * @param joinfield the LHS field used for joining
	 */
	public NestedLoopJoinScan(Scan lhs, Scan rhs, String joinfield, String fieldRhs) {
		this.lhs = lhs;
		this.joinfield = joinfield;
		this.rhs = rhs;
		this.fieldRhs = fieldRhs;
		beforeFirst();
	}

	@Override
	public void beforeFirst() {
		lhs.beforeFirst();
		lhs.next();
		rhs.beforeFirst();
	}

	/**
	 * Move the scan to the next record. The method moves to the next RHS record, if
	 * possible. Otherwise, it moves to the next LHS record and the first RHS
	 * record. If there are no more LHS records, the method returns false.
	 * 
	 * @see simpledb.query.Scan#next()
	 */
	public boolean next() {

		Constant searchkey = lhs.getVal(joinfield);
		while (rhs.next()) {
			if (rhs.getVal(fieldRhs).equals(searchkey)) {
				return true;
			}
		}
		return nextIteration();
	}

	@Override
	public int getInt(String fldname) {
		if (rhs.hasField(fldname))
			return rhs.getInt(fldname);
		else
			return lhs.getInt(fldname);
	}

	@Override
	public String getString(String fldname) {
		if (rhs.hasField(fldname))
			return rhs.getString(fldname);
		else
			return lhs.getString(fldname);
	}

	@Override
	public Constant getVal(String fldname) {
		if (rhs.hasField(fldname))
			return rhs.getVal(fldname);
		else
			return lhs.getVal(fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return rhs.hasField(fldname) || lhs.hasField(fldname);
	}

	@Override
	public void close() {
		lhs.close();
		rhs.close();
	}
	
	public boolean nextIteration() {
		if (lhs.next()) {
			rhs.beforeFirst();
			return next();
		}
		return false;
	}

}
