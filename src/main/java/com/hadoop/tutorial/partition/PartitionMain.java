package com.hadoop.tutorial.partition;


import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *在reduce阶段对数据进行分区
 */
public class PartitionMain {

    public static void main(String[] args) throws Exception{
        if (args.length != 2) {
            System.err.println("Usage: PartitionMain <input path> <output path>");
            System.exit(-1);
        }
        Job job = new Job();
        job.setJarByClass(PartitionMain.class);
        job.setJobName("PartitionMain");

        if(FileSystem.get(job.getConfiguration()).exists(new Path(args[1]))){
            FileSystem.get(job.getConfiguration()).delete(new Path(args[1]),true);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(PartitionMapper.class);
        job.setPartitionerClass(YearPartition.class);
        YearPartition.setMinLastYear(job,2010);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(6); //(start:2010, input_data:2010,2011,2012,2013,2014,2015)
        job.setOutputKeyClass(Text.class);              //注1
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }
}
