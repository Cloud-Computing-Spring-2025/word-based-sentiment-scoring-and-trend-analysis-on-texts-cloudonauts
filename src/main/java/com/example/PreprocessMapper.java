package com.example.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class PreprocessMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Set<String> STOP_WORDS = new HashSet<>(Arrays.asList(
        "the", "and", "a", "to", "of", "in", "i", "it", "for", "on", "you",
        "is", "was", "that", "with", "as", "he", "she", "at", "by", "an", "be", "this", "have"
    ));

    private boolean isHeader = true;

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (isHeader && key.get() == 0) {
            isHeader = false;  // Skip header line
            return;
        }

        String[] fields = value.toString().split(",", 4);

        if (fields.length == 4) {
            String bookID = fields[0].trim();
            String title = fields[1].trim();
            String year = fields[2].trim();
            String text = fields[3].toLowerCase().replaceAll("[^a-z0-9 ]", " ").trim();

            StringTokenizer tokenizer = new StringTokenizer(text);
            StringBuilder cleanedText = new StringBuilder();

            while (tokenizer.hasMoreTokens()) {
                String word = tokenizer.nextToken();
                if (!STOP_WORDS.contains(word) && word.length() > 1) {
                    cleanedText.append(word).append(" ");
                }
            }

            context.write(new Text("("+bookID + "," + year+")"), new Text(cleanedText.toString().trim()));
        }
    }
}