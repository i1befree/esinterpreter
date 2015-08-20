package com.sk.aitech.fetcher;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.feature.StandardScaler;
import org.apache.spark.mllib.feature.StandardScalerModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import scala.Tuple2;

/**
 * Created by i1befree on 15. 6. 30..
 */
public class SparkRegressionTest {
  public static void main(String[] args){
    SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SVM Classifier Example");
    JavaSparkContext sc = new JavaSparkContext(conf);
    String path = "src/main/resources/data/mllib/ridge-data/sample.data";
    JavaRDD<String> data = sc.textFile(path);
    final StandardScaler scaler = new StandardScaler(false, true);

    JavaRDD<LabeledPoint> parsedData = data.map(
        new Function<String, LabeledPoint>() {
          public LabeledPoint call(String line) {
            String[] parts = line.split(",");
            String[] features = parts[1].split(" ");
            double[] v = new double[features.length];

            for (int i = 0; i < features.length; i++) {
              v[i] = Double.parseDouble(features[i]);
            }

            return new LabeledPoint(Double.parseDouble(parts[0]), Vectors.dense(v));
          }
        }
    );

    parsedData.cache();

    final StandardScalerModel stModel = scaler.fit(parsedData.map(new Function<LabeledPoint, Vector>() {
      @Override
      public Vector call(LabeledPoint labeledPoint) throws Exception {
        return labeledPoint.features();
      }
    }).rdd());

    parsedData = parsedData.map(new Function<LabeledPoint, LabeledPoint>() {
      @Override
      public LabeledPoint call(LabeledPoint labeledPoint) throws Exception {
        return new LabeledPoint(labeledPoint.label(), stModel.transform(labeledPoint.features()));
      }
    });

    // Building the model
    int numIterations = 10;
    final LinearRegressionModel model =
        LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);

    // Evaluate model on training examples and compute training error
    JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
        new Function<LabeledPoint, Tuple2<Double, Double>>() {
          public Tuple2<Double, Double> call(LabeledPoint point) {
            double prediction = model.predict(stModel.transform(point.features()));
            System.out.println("prediction : " + prediction + ", label :" + point.label());
            return new Tuple2<Double, Double>(prediction, point.label());
          }
        }
    );

    double MSE = new JavaDoubleRDD(valuesAndPreds.map(
        new Function<Tuple2<Double, Double>, Object>() {
          public Object call(Tuple2<Double, Double> pair) {
            return Math.pow(pair._1() - pair._2(), 2.0);
          }
        }
    ).rdd()).mean();

    System.out.println("training Mean Squared Error = " + MSE);
    // Save and load model
//    model.save(sc.sc(), "myModelPath");
//    LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(), "myModelPath");
  }
}
