package com.hadoop.tutorial.counter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

/**
 * Created by baoyu on 16/10/22.
 */
public class CountNumUsersByCityMapper extends Mapper<Object,Text,NullWritable,NullWritable> {
    public static final String CITY_GROUP_COUNTER = "City";
    public static final String UNKNOWN_GROUP_COUNTER = "Unknon";
    public static final String NULL_OR_EMPTY_GROUP_COUNTER = "Null_or_Empty";

    private String[] cityArray = new String[]{
            "beijing","shanghai","hangzhou","shenzhen"
    };

    private HashSet<String> cities = new HashSet<>(Arrays.asList(cityArray));

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        boolean unknown = true;
        if (value==null || value.getLength()==0){
            context.getCounter(CITY_GROUP_COUNTER,NULL_OR_EMPTY_GROUP_COUNTER).increment(1);
        }
        for (String token:value.toString().toLowerCase().split("\\s")){
            if(cities.contains(token)){
                context.getCounter(CITY_GROUP_COUNTER,token).increment(1);
                unknown = false;
                break;
            }
        }
        if (unknown){
            context.getCounter(CITY_GROUP_COUNTER,UNKNOWN_GROUP_COUNTER).increment(1);
        }
    }
}
