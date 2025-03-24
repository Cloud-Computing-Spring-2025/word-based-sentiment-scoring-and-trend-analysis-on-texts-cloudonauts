package com.example.task2;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.util.PropertiesUtils;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text compositeKey = new Text();
    private static final IntWritable one = new IntWritable(1);
    private static StanfordCoreNLP pipeline;

    @Override
    protected void setup(Context context) {
        if (pipeline == null) {
            Properties props = PropertiesUtils.asProperties(
                "annotators", "tokenize,ssplit,pos,lemma",
                "tokenize.language", "en",
                "ssplit.isOneSentence", "true"
            );
            pipeline = new StanfordCoreNLP(props);
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] parts = line.split("\t", 2);
        if (parts.length < 2) return;

        String header = parts[0].trim();
        String[] headerParts = header.split("_", 3);
        if (headerParts.length < 3) return;

        String bookID = headerParts[0].trim();
        String year = headerParts[1].trim();
        String text = parts[1].trim();

        CoreDocument document = new CoreDocument(text);
        pipeline.annotate(document);

        List<CoreLabel> tokens = document.tokens();
        for (CoreLabel token : tokens) {
            String lemma = token.lemma();
            if (lemma != null && !lemma.isEmpty()) {
                lemma = lemma.toLowerCase();
                if (lemma.matches("[a-zA-Z]+")) {  // filter out numbers and symbols
                    compositeKey.set(bookID + ", " + lemma + ", " + year);
                    context.write(compositeKey, one);
                }
            }
        }
    }
}