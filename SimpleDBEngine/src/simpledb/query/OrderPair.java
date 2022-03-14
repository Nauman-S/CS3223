package simpledb.query;

public class OrderPair {
	private String field;
	private boolean isAsc;

	public OrderPair(String field, boolean isAsc) {
		this.field = field;
		this.isAsc = isAsc;
	}

	public OrderPair(String field) {
		this.field = field;
		this.isAsc = true;
	}

	public String getField() {
		return field;
	}

	public boolean isAsc() {
		return isAsc;
	}

	public String toString() {
		return String.format("%s %s", field, isAsc ? "asc" : "desc");
	}
}
