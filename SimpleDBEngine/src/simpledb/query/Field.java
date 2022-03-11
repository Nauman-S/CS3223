package simpledb.query;

public class Field {
	protected String fldname;

	public Field(String fldname) {
		this.fldname = fldname;
	}

	public String getFldname() {
		return fldname;
	}

	@Override
	public String toString() {
		return fldname;
	}

	public boolean isAggregated() {
		return false;
	}
}
