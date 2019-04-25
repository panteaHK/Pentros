/*
 * The source code is provided as basis for research purposes. It is developed by Mehdi Zitouni (Mehdi.Zitouni@inria.fr).
 * The authors assume no responsibility in any bugs that may exist in the codes.
 * Finally, please contact Mehdi Zitouni (at Mehdi.Zitouni@inria.fr) for any request about the PENTROS algorithm.
 */

import java.io.PrintWriter;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

public final class Pentros {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        System.out.println("All the arguments: " + Arrays.toString(args));
        String miki = "";
        if (args.length < 1) {
            System.err.println("Usage: Pentros Missing parameters");
            System.exit(1);
        }

        String args1 = args[0];
        String args2 = args[1];

        PrintWriter writer = new PrintWriter(args2, "UTF-8");
        SparkConf sparkConf = new SparkConf().setAppName("Pentros");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaStreamingContext ssc = new JavaStreamingContext(ctx, new Duration(5));
        ssc.textFileStream("../mushroom.csv");

        JavaDStream<String> lines = ssc.socketTextStream("../mushroom.csv", 1);
        JavaDStream<Long> NumTr = lines.count();

        lines.print();
        NumTr.print();

//        lines.foreachRDD(x => System.out.println(x.count()));

//        System.out.println(lines.count().compute()+ "PLZZZZZZZZZZZZZZ work");


        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String,String>() {
            @Override
            public Iterator<String> call(String s) {
                return Arrays.asList(SPACE.split(s)).iterator();
            }
        });

        JavaPairDStream<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });


        JavaPairDStream<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        JavaPairDStream<String, Integer> wordCounts = ones.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override public Integer call(Integer i1, Integer i2) throws Exception {
                        return i1 + i2;
                    }
                });


        wordCounts.print();
        counts.print();
        ssc.start();              // Start the computation
        ssc.awaitTermination();


//        List<Tuple2<String, Integer>> output = counts.collect();
//
//        Hashtable<String, Double> items = new Hashtable<>();
//
//
//
//
//
//        java.text.DecimalFormat df = new java.text.DecimalFormat("0.##");
//
//        for (Tuple2<?, ?> tuple : output) {
//            JavaPairDStream<Double> Entropy = -(( (double)tuple._2().hashCode() / NumTr) * ((double) Math.log((double) tuple._2().hashCode() / NumTr)))
//                    - ((double) (NumTr - tuple._2().hashCode()) / NumTr) * ((double) Math.log((double) (NumTr - tuple._2().hashCode()) / NumTr));
//            items.put((String) tuple._1(), Entropy);
//            writer.println(tuple._1() + ": " + tuple._2() + " \t --> Entropy : " + df.format(Entropy));
//        }
//        String PossibleMiki = "";
//        Integer k = Integer.parseInt(args1);
//        for (int i = 1; i <= k; i++) {
//            Double maxValue = Double.MIN_VALUE;
//            for (Map.Entry<String, Double> entry : items.entrySet()) {
//                if (entry.getValue() > maxValue) {
//                    maxValue = entry.getValue();
//                    PossibleMiki = entry.getKey();
//                }
//            }
//            // System.out.println("\n The miki with k = "+ k +" is : '"+ miki +"' And its joint entropy is : "+df.format(maxValue));}
//            Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
//                @Override
//                public Integer call(Integer i1, Integer i2) {
//                    return i1 + i2;
//                }
//            };
//            JavaPairDStream<String, Integer> MikiCount = counts.reduceByKeyAndWindow(reduceFunc, Durations.seconds(30), Durations.seconds(10));
//            MikiCount.print();
//            writer.close();
//            System.out.println("\n Output Files Created \n \n");
//            ssc.start();
//            ssc.awaitTermination();
//            ctx.stop();
//        }
    }
}