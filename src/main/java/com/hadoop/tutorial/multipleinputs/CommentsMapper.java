package com.hadoop.tutorial.multipleinputs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by baoyu on 16/11/6.
 */
public class CommentsMapper extends Mapper<Object,Text,Text,Text> {

    private Text outKey = new Text();
    private Text outValue = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {


            String[] lineInput =  value.toString().split("#");   //newsId#commentId#commentContent
            if(lineInput.length==3){
                outKey.set(lineInput[0]);
                outValue.set("Com"+lineInput[1]+"#"+lineInput[2]);
                context.write(outKey,outValue);
             }

    }
}
