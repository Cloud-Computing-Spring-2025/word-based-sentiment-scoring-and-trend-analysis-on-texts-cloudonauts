package com.example;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for word frequency analysis with lemmatization.
 */
public class LemmaFrequencyReducer extends Reducer<LemmaFrequencyMapper.LemmaKey, IntWritable, Text, Text> {
    
    @Override
    public void reduce(LemmaFrequencyMapper.LemmaKey key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }

        Text outputKey = new Text(key.getBookId());
        Text outputValue = new Text(key.getLemma() + "\t" + key.getYear() + "\t" + sum);

        context.write(outputKey, outputValue);
    }
}