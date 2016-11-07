package com.hadoop.tutorial.count.average;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by baoyu on 16/10/22.
 */
public class AverageMapper extends Mapper<Object,Text,IntWritable,CountAverageTuple> {

    private IntWritable outHour = new IntWritable();
    private CountAverageTuple outCountAverage = new CountAverageTuple();
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] inputLine = value.toString().split("#");
        try {

            if (inputLine.length >= 2) {
                int hour = sdf.parse(inputLine[1]).getHours();
                outHour.set(hour);
                outCountAverage.setAverage(inputLine[2].length());
                outCountAverage.setCount(1);
                context.write(outHour,outCountAverage);
            }
        }catch (ParseException pe){
            pe.printStackTrace();
        }
    }
}
