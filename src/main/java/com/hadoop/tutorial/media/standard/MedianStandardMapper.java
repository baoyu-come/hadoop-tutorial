package com.hadoop.tutorial.media.standard;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by baoyu on 16/10/22.
 */
public class MedianStandardMapper extends Mapper<Object,Text,IntWritable,IntWritable> {

    private IntWritable outHour = new IntWritable();
    private IntWritable outCommonLength = new IntWritable();
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] inputLine = value.toString().split("#");
        try {

            if (inputLine.length >= 2) {
                int hour = sdf.parse(inputLine[1]).getHours();
                outHour.set(hour);
                outCommonLength.set(inputLine[2].length());
                context.write(outHour,outCommonLength);
            }
        }catch (ParseException pe){
            pe.printStackTrace();
        }
    }
}
