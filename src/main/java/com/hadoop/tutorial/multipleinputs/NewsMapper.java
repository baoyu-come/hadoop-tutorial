package com.hadoop.tutorial.multipleinputs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by baoyu on 16/11/6.
 */
public class NewsMapper extends Mapper<Object,Text,Text,Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lineInput =  value.toString().split("#");   //newsId#newsContent
            if(lineInput.length==2){
                outKey.set(lineInput[0]);
                outValue.set("News"+value);
                context.write(outKey,outValue);
            }

    }
}
