
package com.example.task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class PreprocessReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder cleanedText = new StringBuilder();
        
        for (Text val : values) {
            cleanedText.append(val.toString()).append(" ");
        }

        // Fix: Ensure correct delimiter between fields
        String output = key.toString() + "|" + cleanedText.toString().trim();
        context.write(new Text(output), null);
    }
}
