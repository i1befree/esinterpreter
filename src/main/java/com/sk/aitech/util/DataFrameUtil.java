package com.sk.aitech.util;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by i1befree on 15. 6. 30..
 */
public final class DataFrameUtil {
  /**
   * DataFrame 생성용 유틸리티 클래스. 현재는 String 필드만 지원토록 정의되며 추후 변경 필요
   * @param fieldNames
   * @param data
   * @return
   */
  public static DataFrame makeDataFrame(SQLContext sc, List<String> fieldNames, List<String> data){
    List<StructField> fields = new ArrayList<>();

    for(String fieldName : fieldNames){
      fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
    }

    StructType schema = DataTypes.createStructType(fields);
    JavaSparkContext javaSparkContext = new JavaSparkContext(sc.sparkContext());
    JavaRDD<String> dataRdd = javaSparkContext.parallelize(data);

    JavaRDD<Row> rowRDD = dataRdd.map(
        new Function<String, Row>() {
          public Row call(String record) throws Exception {
            String[] fields = record.split("\t");
            return RowFactory.create(fields);
          }
        });

    //테스트용.
    DataFrame dataFrame = sc.createDataFrame(rowRDD, schema);

    return dataFrame;
  }

  public static JavaRDD<Vector> toVectorRdd(DataFrame df, String[] columnNames) {
    List<Column> cols = new ArrayList<>();

    for(String columnName : columnNames){
      cols.add(df.col(columnName.trim()));
    }

    return df.select(cols.toArray(new Column[0])).javaRDD().map(
        new Function<Row, Vector>() {
          public Vector call(Row row) {
            StructField[] fields = row.schema().fields();
            double[] values = new double[fields.length];

            for(int i = 0 ; i < fields.length ; i++){
              if(fields[i].dataType() instanceof StringType){
                values[i] = Double.parseDouble(row.getString(i));
              }else if(fields[i].dataType() instanceof DoubleType){
                values[i] = row.getDouble(i);
              }else if(fields[i].dataType() instanceof IntegerType){
                values[i] = (double)row.getInt(i);
              }
            }

            return Vectors.dense(values);
          }
        }
    );
  }

  public static JavaDoubleRDD extractDoubleColumn(DataFrame df, String columnName) {
    Column column = df.col(columnName);

    return new JavaDoubleRDD(df.select(column).javaRDD().map(
        new Function<Row, Object>() {
          public Object call(Row row) {
            return Double.parseDouble(row.getString(0));
          }
        }
    ).rdd());
  }

  public static JavaRDD<LabeledPoint> convertLabelRdd(DataFrame df, String labelField, String[] featureFields){
    Column[] columns = new Column[featureFields.length + 1];

    columns[0] = df.col(labelField);

    for(int i = 0 ; i < featureFields.length ; i++){
      columns[i + 1] = df.col(featureFields[i]);
    }

    return df.select(columns).toJavaRDD().map(new Function<Row, LabeledPoint>() {
      @Override
      public LabeledPoint call(Row row) throws Exception {
        StructField[] fields = row.schema().fields();
        double labelField = 0;
        double[] values = new double[fields.length - 1];

        for(int i = 0 ; i < fields.length ; i++){
          if(fields[i].dataType() instanceof StringType){
            if(i == 0)
              labelField = Double.parseDouble(row.getString(i));
            else
              values[i - 1] = Double.parseDouble(row.getString(i));
          }else if(fields[i].dataType() instanceof DoubleType){
            if(i == 0)
              labelField = row.getDouble(i);
            else
              values[i - 1] = row.getDouble(i);
          }else if(fields[i].dataType() instanceof IntegerType){
            if(i == 0)
              labelField = (double)row.getInt(i);
            else
              values[i - 1] = (double)row.getInt(i);
          }
        }

        return new LabeledPoint(labelField, Vectors.dense(values));
      }
    });
  }
}
