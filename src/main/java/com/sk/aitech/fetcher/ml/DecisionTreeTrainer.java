package com.sk.aitech.fetcher.ml;

import com.sk.aitech.util.DataFrameUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by i1befree on 15. 7. 21..
 */
public class DecisionTreeTrainer {
  DataFrame trainData;
  String labelField;
  String[] featureFields;
  Map<String, Object> options;
  DecisionTreeModel model;

  public DecisionTreeTrainer(DataFrame trainData, String labelField, String[] featureFields, Map<String, Object> options){
    this.trainData = trainData;
    this.labelField = labelField;
    this.featureFields = featureFields;
    this.options = options;
  }

  public void train(){
    int numClasses = 10;
    String impurity = "gini";
    int maxDepth = 5;
    int maxBins = 32;
    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
    
    if(this.options.get("numClasses") != null){
      numClasses = Integer.parseInt((String) this.options.get("numClasses"));
    }

    if(this.options.get("impurity") != null){
      impurity = (String)this.options.get("impurity");
    }

    if(this.options.get("maxDepth") != null){
      maxDepth = Integer.parseInt((String) this.options.get("maxDepth"));
    }

    if(this.options.get("maxBins") != null){
      maxBins = Integer.parseInt((String) this.options.get("maxBins"));
    }

    JavaRDD<LabeledPoint> rdd = DataFrameUtil.convertLabelRdd(trainData, labelField, featureFields);

    model = org.apache.spark.mllib.tree.DecisionTree.trainClassifier(rdd, numClasses,
        categoricalFeaturesInfo, impurity, maxDepth, maxBins);
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
