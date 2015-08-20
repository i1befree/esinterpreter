package com.sk.aitech.fetcher.ml;

import com.sk.aitech.util.DataFrameUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.regression.GeneralizedLinearModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Linear model trainer. 모델을 학습 시키거나 로딩한다.
 */
public class LinearModelTrainer implements Serializable{
  DataFrame trainData;
  String method;
  String labelField;
  String[] featureFields;
  Map<String, String> options;
  GeneralizedLinearModel model;

  public LinearModelTrainer(DataFrame trainData, String method, String labelField, String[] featureFields, Map<String, String> options){
    this.trainData = trainData;
    this.method = method;
    this.labelField = labelField;
    this.featureFields = featureFields;
    this.options = options;
  }

  public void train(){
    JavaRDD<LabeledPoint> rdd = DataFrameUtil.convertLabelRdd(trainData, labelField, featureFields);

    if("logistic".equals(method)) {
      //종류를 주지 않으면 Label 에서 종류를 가져온다.
      String numberOfClasses = options.get("numberOfClasses");

      if(numberOfClasses == null)
        numberOfClasses = String.valueOf(trainData.select(trainData.col(labelField)).distinct().count());

      //TODO: Label 의 값을 1씩 증가하는 자연수로 변경할 필요가 있음.
      this.model = new LogisticRegressionWithLBFGS()
          .setNumClasses(Integer.parseInt(numberOfClasses))
          .run(rdd.rdd());
    }else if("svm".equals(method)){
      String numberOfIterations = options.get("numberOfIterations");

      if(numberOfIterations == null)
        numberOfIterations = "100";

      this.model = SVMWithSGD.train(rdd.rdd(), Integer.parseInt(numberOfIterations));
    }else if("regression".equals(method)){
      String numberOfIterations = options.get("numberOfIterations");

      if(numberOfIterations == null)
        numberOfIterations = "50";

      //Interation 값을 받아와야 함.
      this.model = LinearRegressionWithSGD.train(rdd.rdd(), Integer.parseInt(numberOfIterations));
    }
  }

  public DataFrame predict(DataFrame df, SQLContext sc){
    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField(labelField, DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("expected", DataTypes.DoubleType, true));
    StructType schema = DataTypes.createStructType(fields);

    JavaRDD<LabeledPoint> rdd = DataFrameUtil.convertLabelRdd(df, labelField, featureFields);

    JavaRDD<Row> rowRDD = rdd.map(new Function<LabeledPoint, Row>() {
      @Override
      public Row call(LabeledPoint labeledPoint) throws Exception {
        return RowFactory.create(labeledPoint.label(), model.predict(labeledPoint.features()));
      }
    });

    return sc.createDataFrame(rowRDD, schema);
  }
}
