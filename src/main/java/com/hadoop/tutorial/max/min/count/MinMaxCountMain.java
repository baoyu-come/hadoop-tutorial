package com.hadoop.tutorial.max.min.count;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by baoyu on 16/10/22.
 */
public class MinMaxCountMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MinMaxCount <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(MinMaxCountMain.class);
        job.setJobName("MinMaxCount");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MinMaxCountMapper.class);
//        job.setCombinerClass(MinMaxCountReducer.class);
        job.setReducerClass(MinMaxCountReducer.class);

        job.setOutputKeyClass(Text.class);              //注1
        job.setOutputValueClass(MinMaxCountTuple.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
