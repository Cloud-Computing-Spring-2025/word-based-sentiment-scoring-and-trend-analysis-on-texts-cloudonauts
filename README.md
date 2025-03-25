# MapReduce & Hive : Word-Based Sentiment Scoring and Trend Analysis on Historical Texts
In this project, you will analyze sentiment trends in historical literature by processing a collection of digitized texts (e.g., books, plays, letters) from the 18th and 19th centuries. The dataset has to be multiple books, each with essential metadata (book ID, title, and year of publication). Your goal is to build a multi-stage processing pipeline using Hadoop MapReduce in Java and Hive for data processing, analysis, and visualization.

## Objectives

*Data Extraction & Cleaning : Extract raw text data like (book ID, title, publication year) from multiple files. Process each line by converting it to lowercase and removing stop words.
*Word Frequency Analysis : Split sentences into words, then apply lemmatization using a standard NLP library to group variants into their base forms.
*Sentiment Analysis : Assign sentiment scores using established sentiment lexicons (e.g., AFINN or SentiWordNet) and trace these scores back to each book.
*Trend Analysis : Aggregate sentiment scores and word frequencies over larger time intervals (e.g., by decade) to observe long-term trends and potential historical correlations. Visualization is optional
*Bigram Analysis : Use a custom Hive User-Defined Function (UDF) in Java to extract and analyze bigrams from the text data produced after lemmatization in Task 2.

## Setup and Execution

## Task 1: Preprocessing MapReduce Job

## Task 2: Word Frequency Analysis with Lemmatization
### 1. **Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 2. **Build the Code**

Build the code using Maven:

```bash
mvn clean install
```

### 3. **Open Docker Container**

```bash
docker exec -it resourcemanager /bin/bash
```

And create a directory,

```bash
mkdir -p /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 4. **Copy JAR to Docker Container**

Copy the JAR file to the Hadoop ResourceManager container:

```bash
docker cp target/DataCleaningMapReduce-1.0.0.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce
```

### 5. **Move Dataset to Docker Container**

Copy the dataset to the Hadoop ResourceManager container:

```bash
docker cp output/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 6. **Connect to Docker Container**

Access the Hadoop ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 7. **Set Up HDFS**

Create a folder in HDFS for the input dataset:

```bash
hadoop fs -mkdir -p /input/dataset
```

Copy the input dataset to the HDFS folder:

```bash
hadoop fs -put part-r-00000 /input/dataset
```

### 8. **Execute the MapReduce Job**

Run your MapReduce job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/DataCleaningMapReduce-1.0.0.jar com.example.task2.WordFrequencyDriver /input/dataset/part-r-00000 /output_task2
```

### 9. **View the Output**

To view the output of your MapReduce job, use:

```bash
hadoop fs -cat /output_task2/*
```

### 10. **Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /output_task2 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output_task2/ output/Task2/
    ```
3. Commit and push to your repo so that we can able to see your output

## Task 3: Sentiment Scoring
### 1. **Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 2. **Build the Code**

Build the code using Maven:

```bash
mvn clean install
```

### 3. **Open Docker Container**

```bash
docker exec -it resourcemanager /bin/bash
```

And create a directory,

```bash
mkdir -p /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 4. **Copy JAR to Docker Container**

Copy the JAR file to the Hadoop ResourceManager container:

```bash
docker cp target/DataCleaningMapReduce-1.0.0.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce
```

### 5. **Move Dataset to Docker Container**

Copy the dataset to the Hadoop ResourceManager container:

```bash
docker cp output/Task2/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```
### 6. **Move AFINN-111.txt to Docker Container**

Copy the AFINN-111.txt to the Hadoop ResourceManager container:

```bash
docker cp input/AFIN-111.txt resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 7. **Connect to Docker Container**

Access the Hadoop ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 8. **Set Up HDFS**

Create a folder in HDFS for the input dataset:

```bash
hadoop fs -mkdir -p /input/dataset
```

Copy the input dataset to the HDFS folder:

```bash
hadoop fs -put part-r-00000 /input/dataset
```
Copy the AFINN-111.txt to the HDFS folder:

```bash
hadoop fs -put AFINN-111.txt /input/dataset
```

### 9. **Execute the MapReduce Job**

Run your MapReduce job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/DataCleaningMapReduce-1.0.0.jar com.example.task3.SentimentScoringDriver /input/dataset/part-r-00000 /output_task3 /input/dataset/AFINN-111.txt
```

### 10. **View the Output**

To view the output of your MapReduce job, use:

```bash
hadoop fs -cat /output_task3/*
```

### 11. **Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /output_task3 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output_task3/ output/Task3/
    ```
3. Commit and push to your repo so that we can able to see your output

## Task 4: Trend Analysis & Aggregation

### 1. **Start the Hadoop Cluster**

Run the following command to start the Hadoop cluster:

```bash
docker compose up -d
```

### 2. **Build the Code**

Build the code using Maven:

```bash
mvn install
```

### 3. **Open Docker Container**

```bash
docker exec -it resourcemanager /bin/bash
```

And create a directory,

```bash
mkdir -p /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 4. **Copy JAR to Docker Container**

Copy the JAR file to the Hadoop ResourceManager container:

```bash
docker cp target/DataCleaningMapReduce-1.0.0.jar resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce
```

### 5. **Move Dataset to Docker Container**

Copy the dataset to the Hadoop ResourceManager container:

```bash
docker cp output/Task3/part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 6. **Connect to Docker Container**

Access the Hadoop ResourceManager container:

```bash
docker exec -it resourcemanager /bin/bash
```

Navigate to the Hadoop directory:

```bash
cd /opt/hadoop-3.2.1/share/hadoop/mapreduce/
```

### 7. **Set Up HDFS**

Create a folder in HDFS for the input dataset:

```bash
hadoop fs -mkdir -p /input/dataset
```

Copy the input dataset to the HDFS folder:

```bash
hadoop fs -put part-r-00000 /input/dataset
```

### 8. **Execute the MapReduce Job**

Run your MapReduce job using the following command:

```bash
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/DataCleaningMapReduce-1.0.0.jar com.example.task4.TrendDriver /input/dataset/part-r-00000 /output_task4
```

### 9. **View the Output**

To view the output of your MapReduce job, use:

```bash
hadoop fs -cat /output_task4/*
```

### 10. **Copy Output from HDFS to Local OS**

To copy the output from HDFS to your local machine:

1. Use the following command to copy from HDFS:
    ```bash
    hdfs dfs -get /output_task4 /opt/hadoop-3.2.1/share/hadoop/mapreduce/
    ```

2. use Docker to copy from the container to your local machine:
   ```bash
   exit 
   ```
    ```bash
    docker cp resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/output_task4/ output/Task4/
    ```
3. Commit and push to your repo so that we can able to see your output

# Task 5: Bigram Analysis using Hive UDF

---

## Overview

In **Task 5** of our project *"Word-Based Sentiment Scoring and Trend Analysis on Historical Texts"*, we implemented **Bigram Analysis** using a **custom Hive UDF in Java**. The goal was to extract, filter, and analyze bigrams from lemmatized book texts (output from Task 2) and identify patterns in bigram frequency across books and decades.

---

## Input Data

We used the **output of Task 2** 

Each line represents a full book’s lemmatized and cleaned text, with **duplicate words removed**, enabling unique bigram extraction.

## UDF Implementation

Hive UDF Implementation
We wrote a custom Java class that implements Hive’s UDF interface. This function:

Accepts a string of lemmatized text

Splits it into words

Extracts all consecutive word pairs (bigrams)

Returns a list of bigrams

This UDF was compiled into a JAR file and registered in Hive using ADD JAR and CREATE TEMPORARY FUNCTION.

## Compilation & JAR Creation

### Project Structure
```
src/
└── main/
    └── java/
        └── com/example/hive/udf/BigramUDF.java
```

### Commands

```bash
# Copy source into Docker
docker cp src/main/java hive-server:/opt/
```

```bash
# Compile & package JAR inside container
docker exec -it hive-server bash
```

```bash
cd /opt
mkdir build
javac -cp "$(hadoop classpath):/opt/hive/lib/*" -d build java/com/example/hive/udf/BigramUDF.java
jar -cvf bigram-udf.jar -C build/ .
```

---

## Hive Integration

### Launch Hive Shell

```bash
hive
```

### Register UDF

```sql
ADD JAR /opt/bigram-udf.jar;
CREATE TEMPORARY FUNCTION extract_bigrams AS 'com.example.hive.udf.BigramUDF';
```

---

## Bigram Query Explanations

### 1. Extracting Top 50 Bigrams (Overall)

We ran a query to:
- Call the UDF using `LATERAL VIEW explode(...)`
- Filter out bigrams where either word was a stopword (like “the”, “and”, “is”)
- Group by bigram and count their occurrences
- Return the top 50 most frequent bigrams across all books

### 2. Top 5 Bigrams per Book (Grouped)

To prevent one large book from dominating the results, we used the `ROW_NUMBER()` window function:
- After grouping by `book_id` and `bigram`, we sorted by frequency
- `ROW_NUMBER()` assigned a rank to each bigram within a book
- We filtered to keep only the top 5 per book

This helped show what themes or repeated expressions appeared most prominently in each individual book.

### 3. Top 5 Bigrams per Decade (Grouped)

Similarly, we computed the top 5 bigrams for each **decade**:
- We converted `year` to a `decade` (e.g., 1850 → 1850s)
- Grouped by `decade` and `bigram`
- Counted frequency and applied `ROW_NUMBER()` to pick top 5 bigrams per decade

---

## Output Storage (HDFS)

We used `INSERT OVERWRITE DIRECTORY` statements in Hive to write the results into:

- `/user/hive/output/top_bigrams_per_book`
- `/user/hive/output/top_bigrams_per_decade`
- `/user/hive/output/top_bigrams_overall`

We validated these outputs using HDFS commands like `hdfs dfs -cat ...` and previewed them from the Hive container.

---

## Observations & Insights

- Common bigrams such as `"elizabeth darcy"` or `"white whale"` appeared in the most frequent list, reflecting core themes of the books.
- Without stopword filtering, many meaningless bigrams like `"the and"` or `"is to"` dominated the output — which we resolved with a carefully crafted stopword filter.
- Using `ROW_NUMBER()` ensured fairness and made the analysis book-specific and decade-specific.
- Deduplicated input reduced bias from high-frequency words and highlighted semantically richer patterns.

---

## Challenges Faced

| Challenge | Solution |
|----------|----------|
| **UDTF not allowed in WHERE/GROUP BY** | Used `LATERAL VIEW explode()` properly with subqueries |
| **UDF compile errors (missing UDF class)** | Added `hive-exec` dependency to `pom.xml` |
| **Meaningless bigrams flooding results** | Implemented strong stopword filtering on both words in each bigram |
| **0 results when using unique input** | Removed `HAVING frequency > 1` condition to allow rare bigrams |
| **Query only returning one book or decade** | Used `ROW_NUMBER()` with `PARTITION BY` to extract top-N per group |

---

## Conclusion

Task 5 effectively demonstrated:
- How to extend Hive using custom Java UDFs
- How to analyze co-occurring word pairs from historical text
- The importance of linguistic filtering and data structuring for meaningful insights

With the outputs stored in HDFS and queries documented, this step completes the pipeline from raw historical text to structured insight.

---
