package simpledb.materialize;

import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.record.RID;
import simpledb.record.TableScan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Stack;

public class HashJoinScan implements Scan {
    private List<TempTable> buildRelation, probeRelation;
    private String fldname1, fldname2;
    private int partitionNumber;
    private TableScan buildScan, probeScan;
    private HashMap<Constant, List<RID>> joinTable;
    private Stack<RID> matchingRid;

    public HashJoinScan(List<TempTable> buildRelation, List<TempTable> probeRelation, String fldname1,
            String fldname2) {
        this.buildRelation = buildRelation;
        this.probeRelation = probeRelation;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.partitionNumber = 0;
        this.joinTable = new HashMap<>();
        this.matchingRid = new Stack<>();
        beforeFirst();
    }

    @Override
    public void beforeFirst() {
        partitionNumber = 0;
        buildHashTable();
        if (buildScan != null)
            buildScan.beforeFirst();
        if (probeScan != null)
            probeScan.beforeFirst();
    }

    @Override
    public boolean next() {

        if (joinTable.isEmpty() || probeScan == null || buildScan == null) {
            return false;
        }

        if (matchingRid != null && !matchingRid.isEmpty()) {
            buildScan.moveToRid(matchingRid.pop());
            return true;
        }

        while (true) {
            while (probeScan.next()) {
                Constant key = probeScan.getVal(fldname2);
                if (joinTable.containsKey(key)) {
                    matchingRid.clear();
                    matchingRid.addAll(joinTable.get(key));
                    buildScan.moveToRid(matchingRid.pop());
                    return true;
                }
            }
            buildScan.close();
            probeScan.close();
            buildHashTable();
            if (joinTable.isEmpty() || probeScan == null || buildScan == null) {
                return false;
            }
        }
    }

    @Override
    public int getInt(String fldname) {
        if (buildScan.hasField(fldname))
            return buildScan.getInt(fldname);
        else
            return probeScan.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (buildScan.hasField(fldname))
            return buildScan.getString(fldname);
        else
            return probeScan.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        if (buildScan.hasField(fldname))
            return buildScan.getVal(fldname);
        else
            return probeScan.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return buildScan.hasField(fldname) || probeScan.hasField(fldname);
    }

    @Override
    public void close() {
        return;
    }

    private void buildHashTable() {
        joinTable = new HashMap<>();
        matchingRid = new Stack<>();

        TempTable currentPartition = null;
        TempTable currentProbePartition = null;
        while (currentPartition == null || currentProbePartition == null) {
            if (partitionNumber >= buildRelation.size())
                return;
            currentPartition = buildRelation.get(partitionNumber);
            currentProbePartition = probeRelation.get(partitionNumber);
            partitionNumber++;
        }
        probeScan = (TableScan) currentProbePartition.open();
        buildScan = (TableScan) currentPartition.open();
        buildScan.beforeFirst();
        while (buildScan.next()) {
            Constant constant = buildScan.getVal(fldname1);
            List<RID> temp;
            if (joinTable.containsKey(constant)) {
                temp = joinTable.get(constant);
            } else {
                temp = new ArrayList<>();
            }
            temp.add(buildScan.getRid());
            joinTable.put(constant, temp);
        }
    }
}
