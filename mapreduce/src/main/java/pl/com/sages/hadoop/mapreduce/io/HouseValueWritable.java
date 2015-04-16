package pl.com.sages.hadoop.mapreduce.io;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * House Value Writable
 */
public class HouseValueWritable implements Writable {
    private long count;
    private int landArea;
    private int grossArea;
    private int yearBuilt;
    private int salePrice;

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public int getLandArea() {
        return landArea;
    }

    public void setLandArea(int landArea) {
        this.landArea = landArea;
    }

    public int getGrossArea() {
        return grossArea;
    }

    public void setGrossArea(int grossArea) {
        this.grossArea = grossArea;
    }

    public int getYearBuilt() {
        return yearBuilt;
    }

    public void setYearBuilt(int yearBuilt) {
        this.yearBuilt = yearBuilt;
    }

    public int getSalePrice() {
        return salePrice;
    }

    public void setSalePrice(int salePrice) {
        this.salePrice = salePrice;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeInt(landArea);
        dataOutput.writeInt(grossArea);
        dataOutput.writeInt(yearBuilt);
        dataOutput.writeInt(salePrice);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readLong();
        landArea = dataInput.readInt();
        grossArea = dataInput.readInt();
        yearBuilt = dataInput.readInt();
        salePrice = dataInput.readInt();
    }
}
