# spark-tfidf

Spark SQL TFIDF version

The corpus is formed by 2-Grams of the values input column, instead of full words, so it provides higher accuracy.
Suitable for small sentences, names, addresses, etc

Functionality is provided in the TextComparator class. Look at TextComparatorText for examples of usage.


Main functionality:


Adds to a DataFrame, the similarities found on another DataFrame's column

```

// df1
val docs: RDD[String] = spark.sparkContext.parallelize(Array("ruben gomez", "catherine boothman", "matt beckingham", "stephen obrien", "rubencio gonsalez"))
val df = docs.toDF("name")

// df2
val queryDocs = Array("rubens gomes", "catherin zothman", "raul gomsalez", "david beckham", "steven obrian", "matt beckingham")
val queryRdd: RDD[String] = spark.sparkContext.parallelize(queryDocs)
val queryDf = queryRdd.toDF("query")

// Add similarities from df2 to df1 with a comparison threshold >= 0.8
val comparator = new TextComparator(df, "name")
comparator.addSimilarities(queryDf, "query", threshold = 0.8).show

+----------------+--------------------+
|           query|        similarities|
+----------------+--------------------+
|catherin zothman|[catherine boothman]|
| matt beckingham|   [matt beckingham]|
|   raul gomsalez|                  []|
|   steven obrian|                  []|
|   david beckham|                  []|
|    rubens gomes|       [ruben gomez]|
+----------------+--------------------+


```


Shows the similarity matrix between 2 DataFrames

```

/ df1
val docs: RDD[String] = spark.sparkContext.parallelize(Array("ruben gomez", "catherine boothman", "matt beckingham", "stephen obrien", "rubencio gonsalez"))
val df = docs.toDF("name")

// df2
val queryDocs = Array("rubens gomes", "catherin zothman", "raul gomsalez", "david beckham", "steven obrian", "matt beckingham")
val queryRdd: RDD[String] = spark.sparkContext.parallelize(queryDocs)
val queryDf = queryRdd.toDF("query")

// Add similarities from df2 to df1 with a comparison threshold >= 0.8
val comparator = new TextComparator(df, "name")
val result: SimilarityMatrix = comparator.calculateSimilarities(queryDf, "query")
result.show


|               doc|catherine boothman|matt beckingham|ruben gomez|rubencio gonsalez|stephen obrien|
+------------------+------------------+---------------+-----------+-----------------+--------------+
|  catherin zothman|           0.83020|        0.10804|    0.05464|          0.00000|       0.11339|
|     david beckham|           0.04766|        0.65011|    0.02963|          0.01906|       0.00000|
|   matt beckingham|           0.12392|        1.00000|    0.01926|          0.01239|       0.00000|
|     raul gomsalez|           0.00000|        0.00000|    0.44595|          0.54836|       0.00000|
|      rubens gomes|           0.00000|        0.01885|    0.81163|          0.38702|       0.03958|
|     steven obrian|           0.13536|        0.00000|    0.09399|          0.01542|       0.73856|
+------------------+------------------+---------------+-----------+-----------------+--------------+

```