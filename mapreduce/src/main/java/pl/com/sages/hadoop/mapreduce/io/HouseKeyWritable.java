package pl.com.sages.hadoop.mapreduce.io;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * House Key Writable
 */
public class HouseKeyWritable implements WritableComparable {
    private String type;
    private String hood;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHood() {
        return hood;
    }

    public void setHood(String hood) {
        this.hood = hood;
    }

    @Override
    public int compareTo(Object o) {
        HouseKeyWritable house = (HouseKeyWritable)o;
        int result = type.compareTo(house.getType());
        // If the same compare hood
        if (result == 0) {
            return hood.compareTo(house.getHood());
        }
        return result;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeUTF(hood);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        type = dataInput.readUTF();
        hood = dataInput.readUTF();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        int th = type.hashCode();
        int hh = hood.hashCode();
        result = prime * result + th;
        result = prime * result + (int) (hh ^ (hh >>> 32));
        return result;
    }
}
