package com.hadoop.tutorial.count.average;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by baoyu on 16/10/22.
 */
public class CountAverageTuple implements Writable {

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getAverage() {
        return average;
    }

    public void setAverage(long average) {
        this.average = average;
    }

    private long count;
    private long average;
    @Override
    public void write(DataOutput dataOutput) throws IOException {
             dataOutput.writeLong(count);
             dataOutput.writeLong(average);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        count = dataInput.readLong();
        average = dataInput.readLong();

    }

    @Override
    public String toString() {
        return count+"\t"+average;
    }
}
