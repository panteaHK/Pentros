# Pentros
An algorithm for Maximally Informative k-Itemsets mining from data streams


## Requirements


PENTROS works with [Apache Spark](http://spark.apache.org/). In order to run PENTROS you must download and install [Apache Spark 1.6.1](http://spark.apache.org/news/spark-1-6-1-released.html).



## Building



The code is written in Java and we used maven to build it, Use the given [pom.xml](pom.xml) file to build an executable jar containing all the dependencies.


## Datasets


We used a built-in streaming source of Spark Streaming, fed from two real-life datasets. The [Clue Web](http://www.lemurproject.org/clueweb09/) and the [English Wikipedia Articles](https://en.wikipedia.org/wiki/Wikipedia:Database_download) datasets.


## Usage


To run PENTROS please note : 

1 - Set the data location on the machine in the main class : 
    streamingContext.fileStream [ KeyClass, ValueClass, InputFormatClass ] (dataDirectory)
    
2 - Run the following command : 
    ./bin/spark-submit --class Main_Class Jar_File K_size Out_Put_File
```sh
# To use DPiSAX class 


```
