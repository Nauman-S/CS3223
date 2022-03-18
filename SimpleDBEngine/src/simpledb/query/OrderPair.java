package simpledb.query;

/**
 * Order pair.
 */
public class OrderPair {
    private String field;
    private boolean isAsc;

    /**
     * Instantiates a new Order pair.
     *
     * @param field the field
     * @param isAsc the is asc
     */
    public OrderPair(String field, boolean isAsc) {
        this.field = field;
        this.isAsc = isAsc;
    }

    /**
     * Instantiates a new Order pair.
     *
     * @param field the field
     */
    public OrderPair(String field) {
        this.field = field;
        this.isAsc = true;
    }

    /**
     * Gets field.
     *
     * @return the field
     */
    public String getField() {
        return field;
    }

    /**
     * Is asc boolean.
     *
     * @return the boolean
     */
    public boolean isAsc() {
        return isAsc;
    }

    public String toString() {
        return String.format("%s %s", field, isAsc ? "asc" : "desc");
    }
}
