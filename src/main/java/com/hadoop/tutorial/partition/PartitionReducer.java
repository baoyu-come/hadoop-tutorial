package com.hadoop.tutorial.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * Created by baoyu on 16/11/6.
 */
public class PartitionReducer extends Reducer<IntWritable,Text,Text,NullWritable> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text t: values) {
            System.out.println("====:"+t);
            context.write(t,NullWritable.get());
        }
    }
}
