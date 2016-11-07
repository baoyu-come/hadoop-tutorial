package com.hadoop.tutorial.count.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by baoyu on 16/10/22.
 */
public class AverageReducer extends Reducer<IntWritable,CountAverageTuple,IntWritable,CountAverageTuple> {
    private CountAverageTuple result = new CountAverageTuple();

    @Override
    protected void reduce(IntWritable key, Iterable<CountAverageTuple> values, Context context) throws IOException, InterruptedException {
        long sum = 0;
        long count = 0;
        for (CountAverageTuple val:values) {
            sum += val.getCount()* val.getAverage();
            count += val.getCount();
        }
        result.setCount(count);
        result.setAverage(sum / count);
        context.write(key,result);
    }
}
