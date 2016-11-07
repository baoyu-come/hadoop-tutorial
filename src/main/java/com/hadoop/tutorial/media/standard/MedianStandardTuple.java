package com.hadoop.tutorial.media.standard;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by baoyu on 16/10/22.
 */
public class MedianStandardTuple implements Writable{

    private float median ;
    private float standard;

    public float getMedian() {
        return median;
    }

    public void setMedian(float median) {
        this.median = median;
    }

    public float getStandard() {
        return standard;
    }

    public void setStandard(float standard) {
        this.standard = standard;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeFloat(median);
            dataOutput.writeFloat(standard);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
            median = dataInput.readFloat();
            standard = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return median+"\t"+standard;
    }
}
