package simpledb.query;

/**
 * Field.
 */
public class Field {
    /**
     * The field name.
     */
    protected String fldname;

    /**
     * Instantiates a new Field.
     *
     * @param fldname the fldname
     */
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

    /**
     * Returns true if the field is aggregated.
     *
     * @return the boolean
     */
    public boolean isAggregated() {
        return false;
    }
}
