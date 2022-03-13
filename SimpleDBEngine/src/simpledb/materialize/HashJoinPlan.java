package simpledb.materialize;

import simpledb.plan.Plan;
import simpledb.query.Constant;
import simpledb.query.Scan;
import simpledb.query.UpdateScan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

public class HashJoinPlan implements Plan {
    private Transaction tx;
    private Plan p1, p2;
    private String fldname1, fldname2;
    private Schema sch = new Schema();
    private int bufferSize;

    public HashJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
        this.tx = tx;
        this.fldname1 = fldname1;
        this.fldname2 = fldname2;
        this.p1 = p1;
        this.p2 = p2;
        sch.addAll(p1.schema());
        sch.addAll(p2.schema());
        this.bufferSize = tx.availableBuffs();
    }

    @Override
    public Scan open() {
        Scan src = p1.open();
        List<TempTable> partition1 = createPartitions(src, p1.schema(), fldname1);
        src.close();
        src = p2.open();
        List<TempTable> partition2 = createPartitions(src, p2.schema(), fldname2);

        return new HashJoinScan(partition1, partition2, fldname1, fldname2);
    }

    @Override
    public int blocksAccessed() {
        return (p1.blocksAccessed() + p2.blocksAccessed());
    }

    @Override
    public int recordsOutput() {
        int maxvals = Math.max(p1.distinctValues(fldname1),
                p2.distinctValues(fldname2));
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

    private List<TempTable> createPartitions(Scan src, Schema schema, String fldname) {
        List<TempTable> temps = new ArrayList<>();
        src.beforeFirst();
        if (!src.next())
            return temps;

        List<UpdateScan> partitions = new ArrayList<>();
        for (int i = 0; i < bufferSize - 1; ++i) {
            temps.add(null);
            partitions.add(null);
        }

        while (copyToPartition(src, partitions, temps, fldname, schema))
            ;

        for (UpdateScan partition : partitions) {
            if (partition != null)
                partition.close();
        }

        return temps;
    }

    private boolean copyToPartition(Scan src, List<UpdateScan> partitions, List<TempTable> temps, String fldname,
            Schema schema) {
        int index = partitionHash(src.getVal(fldname));
        UpdateScan partition = partitions.get(index);
        if (partition == null) {
            TempTable temp = new TempTable(tx, schema);
            temps.set(index, temp);
            partitions.set(index, temp.open());
            partition = partitions.get(index);
        }
        copy(src, partition, schema);
        return src.next();
    }

    private int partitionHash(Constant val) {
        return val.hashCode() % (bufferSize - 1);
    }

    private void copy(Scan src, UpdateScan dest, Schema schema) {
        dest.insert();
        for (String fldname : schema.fields())
            dest.setVal(fldname, src.getVal(fldname));
    }
}
