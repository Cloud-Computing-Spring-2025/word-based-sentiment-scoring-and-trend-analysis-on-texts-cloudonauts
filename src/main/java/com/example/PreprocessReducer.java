
package com.example.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PreprocessReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder aggregatedText = new StringBuilder();

        for (Text val : values) {
            aggregatedText.append(val.toString()).append(" ");
        }

        context.write(key, new Text(aggregatedText.toString().trim()));
    }
}