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

## Task 3: Sentiment Scoring

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
docker cp output/Task23part-r-00000 resourcemanager:/opt/hadoop-3.2.1/share/hadoop/mapreduce/
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
hadoop jar /opt/hadoop-3.2.1/share/hadoop/mapreduce/DataCleaningMapReduce-1.0.0.jar com.example.task3.TrendDriver /input/dataset/part-r-00000 /output_task4
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
