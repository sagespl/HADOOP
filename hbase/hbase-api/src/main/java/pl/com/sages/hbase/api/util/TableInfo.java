package pl.com.sages.hbase.api.util;

import org.apache.hadoop.hbase.TableName;

import static org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;

public class TableInfo {

    private TableName name;
    private boolean enabled;
    private double averageLocality;
    private CompactionState compaction;

    public TableName getName() {
        return name;
    }

    public void setName(final TableName name) {
        this.name = name;
    }

    public double getAverageLocality() {
        return averageLocality;
    }

    public void setAverageLocality(final double averageLocality) {
        this.averageLocality = averageLocality;
    }

    public CompactionState getCompaction() {
        return compaction;
    }

    public void setCompaction(final CompactionState compaction) {
        this.compaction = compaction;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }
}
