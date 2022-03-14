package simpledb.plan;

import simpledb.query.NestedLoopJoinScan;
import simpledb.query.Scan;
import simpledb.record.Schema;

public class NestedLoopJoinPlan extends Plan {
	private Plan p1, p2;
	private String joinfield1, joinfield2;
	private Schema sch = new Schema();

	/**
	 * Implements the join operator, using the specified LHS and RHS plans.
	 * 
	 * @param p1        the left-hand plan
	 * @param p2        the right-hand plan
	 * @param joinfield the left-hand field used for joining
	 * @param fieldRhs  the right-hand field name used for joining
	 */
	public NestedLoopJoinPlan(Plan p1, Plan p2, String joinfield, String fieldRhs) {
		this.p1 = p1;
		this.p2 = p2;
		this.joinfield1 = joinfield;
		this.joinfield2 = fieldRhs;
		sch.addAll(p1.schema());
		sch.addAll(p2.schema());
	}

	@Override
	public Scan open() {
		Scan s1 = p1.open();
		Scan s2 = p2.open();

		return new NestedLoopJoinScan(s1, s2, joinfield1, joinfield2);
	}

	@Override
	public int blocksAccessed() {
		return p1.blocksAccessed() + p1.recordsOutput() * p2.blocksAccessed();
	}

	@Override
	public int recordsOutput() {
		int maxvals = Math.max(p1.distinctValues(joinfield1), p2.distinctValues(joinfield2));
		return (p1.recordsOutput() * p2.recordsOutput()) / maxvals;
	}

	@Override
	public int distinctValues(String fldname) {
		if (p1.schema().hasField(fldname))
			return p1.distinctValues(fldname);
		else
			return p2.distinctValues(fldname);
	}

	@Override
	public Schema schema() {
		return sch;
	}

	@Override
	public String format(int indent) {
		String indentStr = "\t".repeat(indent);
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("join using nested loop: %s = %s\n", joinfield1, joinfield2));
		sb.append(String.format("%sleft: {%s}\n", indentStr, p1.format(indent + 1)));
		sb.append(String.format("%sright: {%s}", indentStr, p2.format(indent + 1)));

		return sb.toString();
	}

}
