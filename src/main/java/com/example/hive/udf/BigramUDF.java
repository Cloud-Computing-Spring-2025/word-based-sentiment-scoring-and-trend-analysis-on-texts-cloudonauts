package com.example.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import java.util.List;

public class BigramUDF extends UDF {
    public List<Text> evaluate(Text inputText) {
        List<Text> bigrams = new ArrayList<>();
        if (inputText == null || inputText.toString().trim().isEmpty()) return bigrams;

        String[] words = inputText.toString().toLowerCase().split("\\s+");
        for (int i = 0; i < words.length - 1; i++) {
            bigrams.add(new Text(words[i] + " " + words[i + 1]));
        }
        return bigrams;
    }
}
