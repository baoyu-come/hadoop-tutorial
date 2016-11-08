package com.hadoop.tutorial.multipleoutputs;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;

/**
 * Created by baoyu on 16/11/7.
 */
public class MultipleOutputMapper extends Mapper<Object,Text,Text,NullWritable> {

    private MultipleOutputs<Text,NullWritable> mo = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mo = new MultipleOutputs<Text,NullWritable>(context);
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(value != null && value.toString().trim().length() > 0){
            if(value.toString().contains("hadoop")) {
                    mo.write("multiple",value,NullWritable.get(),"hadoop");
            }

            if(value.toString().contains("pig")) {
                mo.write("multiple",value,NullWritable.get(),"pig");
            }

            if(value.toString().contains("hive")) {
                mo.write("multiple",value,NullWritable.get(),"hive");
            }

            if(value.toString().contains("hbase")) {
                mo.write("multiple",value,NullWritable.get(),"hbase");
            }

        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mo.close();
    }
}
