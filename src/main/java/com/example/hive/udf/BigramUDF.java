package com.example.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.List;

/**
 * Hive UDF to extract bigrams from lemmatized text.
 */
public class BigramUDF extends UDF {

    /**
     * Extracts bigrams from a given text.
     * @param text Input lemmatized text
     * @return List of bigrams
     */
    public List<Text> evaluate(Text text) {
        if (text == null || text.toString().trim().isEmpty()) {
            return null; // Return NULL if no valid input
        }

        String[] words = text.toString().split("\\s+");
        if (words.length < 2) {
            return null; // Not enough words to form bigrams
        }

        List<Text> bigrams = new ArrayList<>();
        for (int i = 0; i < words.length - 1; i++) {
            bigrams.add(new Text(words[i] + " " + words[i + 1]));
        }

        return bigrams;
    }
}