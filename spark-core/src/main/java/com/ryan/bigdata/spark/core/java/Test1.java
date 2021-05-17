package com.ryan.bigdata.spark.core.java;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import scala.Function1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class Test1 {
    public static void main(String[] args) {
        String logfile = "input/word.txt";
        SparkSession spark = SparkSession.builder().appName("Sample").getOrCreate();
        Dataset<String> logData = spark.read().textFile(logfile).cache();

//        long numAs = logData.filter((Function1<String, Object>) s -> s.contains("a")).count();
//        long numBs = logData.filter((Function1<String, Object>) s -> s.contains("b")).count();

//        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

        spark.stop();


    }
}
