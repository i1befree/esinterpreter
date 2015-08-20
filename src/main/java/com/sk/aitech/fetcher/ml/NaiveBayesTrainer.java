package com.sk.aitech.fetcher.ml;

import com.sk.aitech.util.DataFrameUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by i1befree on 15. 7. 21..
 */
public class NaiveBayesTrainer {
  DataFrame trainData;
  String labelField;
  String[] featureFields;
  Map<String, Object> options;
  NaiveBayesModel model;

  public NaiveBayesTrainer(DataFrame trainData, String labelField, String[] featureFields, Map<String, Object> options){
    this.trainData = trainData;
    this.labelField = labelField;
    this.featureFields = featureFields;
    this.options = options;
  }

  public void train(){
    double lambda = 1.0;
    String modelType = "multinomial";
    
    if(options.get("lambda") != null){
      lambda = Double.parseDouble((String)options.get("lambda"));
    }

    if(options.get("modeltype") != null){
      modelType = (String)options.get("lambda");
    }

    JavaRDD<LabeledPoint> rdd = DataFrameUtil.convertLabelRdd(trainData, labelField, featureFields);

    model = org.apache.spark.mllib.classification.NaiveBayes.train(
        rdd.rdd(),
        lambda,
        modelType
    );
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
