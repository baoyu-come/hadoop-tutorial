package com.hadoop.tutorial.max.min.count;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by baoyu on 16/10/22.
 */
public class MinMaxCountTuple implements Writable{

    private Date min = new Date();

    private Date max = new Date();

    private long count = 0 ;

    public Date getMin() {
        return min;
    }

    public void setMin(Date min) {
        this.min = min;
    }

    public Date getMax() {
        return max;
    }

    public void setMax(Date max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");




    @Override
    public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeLong(min.getTime());
            dataOutput.writeLong(max.getTime());
            dataOutput.writeLong(count);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            min = new Date(dataInput.readLong());
            max = new Date(dataInput.readLong());
            count = dataInput.readLong();
    }

    @Override
    public String toString() {
        return sdf.format(min)+"\t"+sdf.format(max)+"\t"+count;
    }
}
