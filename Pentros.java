/*
 * The source code is provided as basis for research purposes. It is developed by Mehdi Zitouni (Mehdi.Zitouni@inria.fr). 
 * The authors assume no responsibility in any bugs that may exist in the codes. 
 * Finally, please contact Mehdi Zitouni (at Mehdi.Zitouni@inria.fr) for any request about the PENTROS algorithm.
 */

package Pentros;

import java.io.PrintWriter;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Hashtable;
import java.util.Map;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public final class Pentros {
  private static final Pattern SPACE = Pattern.compile(" ");
  public static void main(String[] args) throws Exception {
      String args1 = args[2];
      String args2 = args[3];
  PrintWriter writer = new PrintWriter(args2, "UTF-8");
  String miki = "regional des das";
    if (args.length < 1) {
      System.err.println("Usage: Pentros Missing parameters");
      System.exit(1);
    }
    SparkConf sparkConf = new SparkConf().setAppName("Pentros");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5));
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0], 1);
    long NumTr = lines.count();
    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) {
        return Arrays.asList(SPACE.split(s));
      }});
    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
        return new Tuple2<String, Integer>(s, 1);
      }});
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }}).sortByKey(true); 
    List<Tuple2<String,Integer>> output = counts.collect();
    Hashtable<String, Double> items = new Hashtable<>();
    java.text.DecimalFormat df = new java.text.DecimalFormat("0.##");
        for (Tuple2<?,?> tuple : output) {
          double Entropy = -(((double)tuple._2().hashCode()/NumTr)*((double)Math.log((double)tuple._2().hashCode()/NumTr)))
                  -((double)(NumTr-tuple._2().hashCode())/NumTr)*((double)Math.log((double)(NumTr-tuple._2().hashCode())/NumTr));
            items.put((String) tuple._1(), Entropy);
            writer.println(tuple._1() + ": " + tuple._2() + " \t --> Entropy : "+ df.format(Entropy));        }
        String PossibleMiki = "";
        Integer k = Integer.parseInt(args1); 
        for(int i = 1; i<= k ; i++){
        Double maxValue = Double.MIN_VALUE;
        for(Map.Entry<String,Double> entry : items.entrySet()) {    
            if(entry.getValue() > maxValue) {
                maxValue = entry.getValue();
                PossibleMiki = entry.getKey();}}
        System.out.println("\n The miki with k = "+ k +" is : '"+ miki +"' And its joint entropy is : "+df.format(maxValue));}    
        Function2<Integer, Integer, Integer> reduceFunc = new Function2<Integer, Integer, Integer>() {
            @Override public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }};
        JavaPairDStream<String, Integer> MikiCount = df.reduceByKeyAndWindow(reduceFunc, Durations.seconds(30), Durations.seconds(10));
        writer.close();
        System.out.println("\n Output Files Created \n \n");
        ssc.start();
        ssc.awaitTermination();         
        ctx.stop();}
}
