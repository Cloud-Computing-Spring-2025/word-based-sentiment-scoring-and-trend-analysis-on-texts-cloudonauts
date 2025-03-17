package com.example;

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations;

/**
 * Mapper for word frequency analysis with lemmatization.
 */
public class LemmaFrequencyMapper extends Mapper<Object, Text, LemmaFrequencyMapper.LemmaKey, IntWritable> {
    
    private StanfordCoreNLP pipeline;
    private final static IntWritable ONE = new IntWritable(1);

    /**
     * Composite Key: bookId, lemma, year
     */
    public static class LemmaKey implements WritableComparable<LemmaKey> {
        private String bookId;
        private String lemma;
        private int year;
        
        public LemmaKey() {} // Default constructor

        public LemmaKey(String bookId, String lemma, int year) {
            this.bookId = bookId;
            this.lemma = lemma;
            this.year = year;
        }

        @Override
        public void write(java.io.DataOutput out) throws IOException {
            WritableUtils.writeString(out, bookId);
            WritableUtils.writeString(out, lemma);
            out.writeInt(year);
        }

        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            bookId = WritableUtils.readString(in);
            lemma = WritableUtils.readString(in);
            year = in.readInt();
        }

        @Override
        public int compareTo(LemmaKey other) {
            int cmp = this.bookId.compareTo(other.bookId);
            if (cmp != 0) return cmp;
            cmp = this.lemma.compareTo(other.lemma);
            return cmp != 0 ? cmp : Integer.compare(this.year, other.year);
        }

        @Override
        public String toString() {
            return bookId + "\t" + lemma + "\t" + year;
        }

        public String getBookId() { return bookId; }
        public String getLemma() { return lemma; }
        public int getYear() { return year; }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Properties props = new Properties();
        props.setProperty("annotators", "tokenize,ssplit,pos,lemma");
        props.setProperty("tokenize.options", "untokenizable=noneKeep");
        props.setProperty("ssplit.eolonly", "true");

        pipeline = new StanfordCoreNLP(props);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                System.out.println("Skipping empty line.");
                return;
            }

            System.out.println("Processing line: " + line);

            // Extract book ID and year correctly
            if (!line.startsWith("(") || !line.contains("),")) {
                System.out.println("Skipping malformed line (Incorrect Key Formatting): " + line);
                return;
            }

            int commaIndex = line.indexOf("),");
            String keyPart = line.substring(1, commaIndex);  // Remove "(" at the start

            String[] keyParts = keyPart.split(",");
            if (keyParts.length != 2) {
                System.out.println("Skipping malformed line (Incorrect Key Parsing): " + line);
                return;
            }

            String bookId = keyParts[0].trim();
            int year;
            try {
                year = Integer.parseInt(keyParts[1].trim());
            } catch (NumberFormatException e) {
                System.out.println("Skipping malformed line (Invalid Year Format): " + line);
                return;
            }

            // Extract the cleaned text after ")," separator
            String cleanedText = line.substring(commaIndex + 2).trim();
            if (cleanedText.isEmpty()) {
                System.out.println("Skipping line with empty text: " + line);
                return;
            }

            // Apply lemmatization using Stanford CoreNLP
            Annotation document = new Annotation(cleanedText);
            pipeline.annotate(document);

            boolean emitted = false;
            for (CoreLabel token : document.get(CoreAnnotations.TokensAnnotation.class)) {
                String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
                if (lemma.matches("[a-zA-Z]+") && lemma.length() > 1) {
                    LemmaKey outputKey = new LemmaKey(bookId, lemma.toLowerCase(), year);
                    System.out.println("Emitting: " + outputKey);
                    context.write(outputKey, ONE);
                    emitted = true;
                }
            }

            if (!emitted) {
                System.out.println("No valid lemmas found in text: " + cleanedText);
            }

        } catch (Exception e) {
            System.err.println("Error processing input: " + value);
            e.printStackTrace();
        }
    }
}