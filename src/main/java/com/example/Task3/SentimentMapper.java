package com.example.task3;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.net.URI;

public class SentimentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private HashMap<String, Integer> sentimentMap = new HashMap<>();
    private Text bookYearKey = new Text();
    private IntWritable sentimentWritable = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            BufferedReader reader = new BufferedReader(new FileReader("AFINN-111.txt"));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split("\t");
                if (tokens.length == 2) {
                    sentimentMap.put(tokens[0].toLowerCase(), Integer.parseInt(tokens[1]));
                }
            }
            reader.close();
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Input: bookID, lemma, year \t count
        String[] parts = value.toString().split("\t");
        if (parts.length != 2) return;

        String[] fields = parts[0].split(",");
        if (fields.length != 3) return;

        String bookID = fields[0].trim();
        String lemma = fields[1].trim().toLowerCase();
        String year = fields[2].trim();
        int count = Integer.parseInt(parts[1]);

        if (sentimentMap.containsKey(lemma)) {
            int sentimentScore = sentimentMap.get(lemma) * count;
            bookYearKey.set(bookID + "," + year);
            sentimentWritable.set(sentimentScore);
            context.write(bookYearKey, sentimentWritable);
        }
    }
}
