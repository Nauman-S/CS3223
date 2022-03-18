package simpledb.parse;

import simpledb.materialize.AggregationFn;
import simpledb.query.OrderPair;
import simpledb.query.Predicate;

import java.util.Collection;
import java.util.List;

/**
 * Data for the SQL <i>select</i> statement.
 *
 * @author Edward Sciore
 */
public class QueryData {
    private boolean isDistinct;
    private List<String> fields;
    private List<AggregationFn> aggfns;
    private Collection<String> tables;
    private Predicate pred;
    private List<String> groupByList;
    private List<OrderPair> orderPairList;

    /**
     * Saves the field and table list and predicate.
     *
     * @param isDistinct    the is distinct
     * @param fields        the fields
     * @param aggfns        the aggfns
     * @param tables        the tables
     * @param pred          the pred
     * @param groupByList   the group by list
     * @param orderPairList the order pair list
     */
    public QueryData(boolean isDistinct, List<String> fields, List<AggregationFn> aggfns,
                     Collection<String> tables, Predicate pred, List<String> groupByList,
                     List<OrderPair> orderPairList) {
        this.isDistinct = isDistinct;
        this.fields = fields;
        this.aggfns = aggfns;
        this.tables = tables;
        this.pred = pred;
        this.groupByList = groupByList;
        this.orderPairList = orderPairList;
    }

    /**
     * Returns the fields mentioned in the select clause.
     *
     * @return a list of field names
     */
    public List<String> fields() {
        return fields;
    }

    /**
     * Returns the tables mentioned in the from clause.
     *
     * @return a collection of table names
     */
    public Collection<String> tables() {
        return tables;
    }

    /**
     * Returns the predicate that describes which records should be in the output
     * table.
     *
     * @return the query predicate
     */
    public Predicate pred() {
        return pred;
    }

    /**
     * Returns the order pairs list.
     *
     * @return the list
     */
    public List<OrderPair> orderPairs() {
        return orderPairList;
    }

    /**
     * Returns the aggregate functions list.
     *
     * @return the list
     */
    public List<AggregationFn> aggfns() {
        return aggfns;
    }

    /**
     * Returns the group by list.
     *
     * @return the list
     */
    public List<String> groupByList() {
        return groupByList;
    }

    /**
     * Returns true if there is an aggregate.
     *
     * @return the boolean
     */
    public boolean isAggregate() {
        return !this.groupByList().isEmpty() || !this.aggfns().isEmpty();
    }

    /**
     * Returns true if the query result is distinct.
     *
     * @return the boolean
     */
    public boolean isDistinct() {
        return this.isDistinct;
    }

    public String toString() {
        String result = "select ";
        for (String fldname : fields)
            result += fldname + ", ";
        for (AggregationFn aggfn : aggfns)
            result += aggfn.fieldName() + ", ";
        result = result.substring(0, result.length() - 2); // remove final comma
        result += " from ";
        for (String tblname : tables)
            result += tblname + ", ";
        result = result.substring(0, result.length() - 2); // remove final comma
        String predstring = pred.toString();
        if (!predstring.equals("")) result += " where " + predstring;
        if (!groupByList.isEmpty()) {
            result += " group by ";
            for (String fldname : groupByList) {
                result += fldname + ", ";
            }
            result = result.substring(0, result.length() - 2); // remove final comma
        }
        if (!orderPairList.isEmpty()) {
            result += " order by ";
            for (OrderPair orderPair : orderPairList) {
                result += orderPair.toString() + ", ";
            }
            result = result.substring(0, result.length() - 2); // remove final comma
        }
        return result;
    }
}
