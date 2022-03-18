package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.OrderPair;
import simpledb.query.Scan;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * A comparator for scans.
 *
 * @author Edward Sciore
 */
public class RecordComparator implements Comparator<Scan> {
    private List<OrderPair> orderPairList;

    /**
     * Create a comparator using the order pairs, using the ordering implied by
     * its iterator.
     *
     * @param orderPairList a list of order pairs
     */
    public RecordComparator(List<OrderPair> orderPairList) {
        this.orderPairList = orderPairList;
    }

    /**
     * Compare the current records of the two specified scans. The sort fields are
     * considered in turn. When a field is encountered for which the records have
     * different values, those values are used as the result of the comparison. If
     * the two records have the same values for all sort fields, then the method
     * returns 0.
     *
     * @param s1 the first scan
     * @param s2 the second scan
     * @return the result of comparing each scan's current record according to the
     * field list
     */
    public int compare(Scan s1, Scan s2) {
        for (OrderPair orderPair : orderPairList) {
            Constant val1 = s1.getVal(orderPair.getField());
            Constant val2 = s2.getVal(orderPair.getField());
            int result;
            if (orderPair.isAsc()) {
                result = val1.compareTo(val2);
            } else {
                result = val2.compareTo(val1);
            }
            if (result != 0) return result;
        }
        return 0;
    }

    @Override
    public String toString() {
        return Arrays.toString(orderPairList.toArray());
    }
}
