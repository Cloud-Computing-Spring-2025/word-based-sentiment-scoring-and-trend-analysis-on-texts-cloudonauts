package com.example.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.example.LemmaFrequencyMapper;
import com.example.LemmaFrequencyReducer;

/**
 * Driver for the Lemma Frequency Analysis MapReduce job.
 */
public class LemmaFrequencyDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: LemmaFrequencyDriver <input path> <output path>");
            return -1;
        }

        // Create and configure a new job
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Lemma Frequency Analysis");

        // Set the driver class
        job.setJarByClass(LemmaFrequencyDriver.class);

        // Set mapper and reducer classes
        job.setMapperClass(LemmaFrequencyMapper.class);
        job.setReducerClass(LemmaFrequencyReducer.class);

        // Set map output key and value classes
        job.setMapOutputKeyClass(LemmaFrequencyMapper.LemmaKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Set final output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // Check if output directory exists and delete it
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Deleted existing output directory: " + outputPath);
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        // Submit the job and wait for completion
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LemmaFrequencyDriver(), args);
        System.exit(exitCode);
    }
}