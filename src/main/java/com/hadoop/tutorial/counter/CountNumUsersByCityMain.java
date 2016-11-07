package com.hadoop.tutorial.counter;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by baoyu on 16/10/22.
 */
public class CountNumUsersByCityMain {

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: CountNumUsersByCityMain <input path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(CountNumUsersByCityMain.class);
        job.setJobName("CountNumUsersByCityMain");

        if(FileSystem.get(job.getConfiguration()).exists(new Path(args[1]))){
            FileSystem.get(job.getConfiguration()).delete(new Path(args[1]),true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setMapperClass(CountNumUsersByCityMapper.class);

        job.setOutputKeyClass(NullWritable.class);              //注1
        job.setOutputValueClass(NullWritable.class);

        int code = job.waitForCompletion(true) ? 0 : 1;
        if(code==0){
            for (Counter counter:job.getCounters().getGroup(CountNumUsersByCityMapper.CITY_GROUP_COUNTER)){
                System.out.println(counter.getDisplayName()+"\t"+counter.getValue());
            }
        }
        if(FileSystem.get(job.getConfiguration()).exists(new Path(args[1]))){
            FileSystem.get(job.getConfiguration()).delete(new Path(args[1]),true);
        }
    }
}
