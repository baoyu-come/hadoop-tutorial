package com.hadoop.tutorial.max.min.count;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by baoyu on 16/10/22.
 */
public class MinMaxCountMapper extends Mapper<Object,Text,Text,MinMaxCountTuple> {
    private Text outUserId = new Text();
    private MinMaxCountTuple outTuple = new MinMaxCountTuple();
    private final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] inputLine = value.toString().split("#");
        if(inputLine.length==2){
            String userId = inputLine[0];
            String dateTime = inputLine[1];
            try {
                Date createTime = sdf.parse(dateTime);
                outTuple.setMax(createTime);
                outTuple.setMin(createTime);
                outTuple.setCount(1);
                outUserId.set(userId);
                context.write(outUserId,outTuple);
            }catch (ParseException pe){
                pe.printStackTrace();
            }
        }


    }
}
