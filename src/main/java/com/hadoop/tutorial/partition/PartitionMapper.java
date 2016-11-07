package com.hadoop.tutorial.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by baoyu on 16/10/22.
 */
public class PartitionMapper extends Mapper<Object,Text,IntWritable,Text> {
    private IntWritable outkey = new IntWritable();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] inputLine = value.toString().split("#");
        if(inputLine.length==3){
            String userId = inputLine[0];
            String dateTime = inputLine[1];
            try {
                outkey.set(Integer.valueOf(dateTime.split("-")[0])); //year
                System.out.println("key :"+outkey.get());
                context.write(outkey,value);
            }catch (Exception pe){
                pe.printStackTrace();
            }
        }


    }
}
