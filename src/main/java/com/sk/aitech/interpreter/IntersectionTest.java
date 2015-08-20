package com.sk.aitech.interpreter;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by i1befree on 15. 7. 17..
 */
public class IntersectionTest {
  public static void main(String[] args){
    JavaSparkContext sc = new JavaSparkContext(new SparkConf().setMaster("local[*]").setAppName("test").set("spark.driver.maxResultSize", "3g"));

    List<String> matrix = new ArrayList<String>();

    for(int i = 0 ; i < 10000 ; i++){
      matrix.add(String.valueOf(i));
    }

    JavaRDD<String> rdd1 = sc.parallelize(matrix);
    rdd1.repartition(16);
    JavaPairRDD<String, String> rdd2 = rdd1.cartesian(rdd1);

    JavaPairRDD<Double, Double> rdd3 = rdd2.mapToPair(new PairFunction<Tuple2<String,String>, Double, Double>() {
      @Override
      public Tuple2<Double, Double> call(Tuple2<String, String> tuple) throws Exception {
        //여기서 추가적인 계산 하는 것도 나쁘지 않음.
        return new Tuple2<>(Double.parseDouble(tuple._1()), Double.parseDouble(tuple._2()));
      }
    });

    rdd3.cache();

    System.out.println("Count : " + rdd3.count());

    List<Tuple2<Double, Double>> results = rdd3.take(10);

    for(Tuple2<Double, Double> result : results){
      System.out.println(result);
    }

    results = rdd3.takeSample(false, 100);

    for(Tuple2<Double, Double> result : results){
      System.out.println(result);
    }
  }
}
