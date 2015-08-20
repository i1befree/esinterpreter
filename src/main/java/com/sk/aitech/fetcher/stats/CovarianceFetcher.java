package com.sk.aitech.fetcher.stats;

import com.google.common.collect.Lists;
import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.IFetcher;
import com.sk.aitech.util.DataFrameUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by i1befree on 15. 7. 7..
 */
@Fetcher(name="covariance", description = "covariance field1, field2")
public class CovarianceFetcher implements IFetcher {
  final Pattern p = Pattern.compile("covariance\\s*\\(?\\s*((\\w+\\s*,?\\s*)+)\\)?");

  String seriesX;
  String seriesY;
  double covValue;

  FetcherContext context;
  String cmdLine;

  public CovarianceFetcher(FetcherContext context) throws FetcherException{
    this.context = context;
    parseCommand(context.getCmd());
    this.cmdLine = context.getCmd();
  }

  private void parseCommand(String cmdLine) throws FetcherException{
    Matcher m = p.matcher(cmdLine);

    if(m.matches()){
      String[] fieldNames = m.group(1).split(",");

      seriesX = fieldNames[0].trim();
      seriesY = fieldNames[1].trim();
    }else
      throw new FetcherException("Invalid command");
  }

  @Override
  public void fetch() throws FetcherException {
    long totalCnt = this.context.getDf().count();

    if(totalCnt > 1){
      Row[] avg = this.context.getDf().selectExpr("avg(" + seriesX + ")", "avg(" + seriesY + ")").collect();

      if(avg != null && avg.length > 0){
        final double field1Avg = avg[0].getDouble(0);
        final double field2Avg = avg[0].getDouble(1);

        double sum = this.context.getDf().toJavaRDD().map(new Function<Row, Double>() {
          @Override
          public Double call(Row row) throws Exception {
            return (Double.parseDouble(row.getString(row.fieldIndex(seriesX))) - field1Avg) * (Double.parseDouble(row.getString(row.fieldIndex(seriesY))) - field2Avg);
          }
        }).reduce(new Function2<Double, Double, Double>() {
          @Override
          public Double call(Double aDouble, Double aDouble2) throws Exception {
            return aDouble + aDouble2;
          }
        });

        covValue = sum / (totalCnt - 1);
      }
    }
  }

  @Override
  public List<String> getRows() {
    List<String> rows = Lists.newArrayList(String.valueOf(covValue));
    return rows;
  }

  @Override
  public List<String> getFields() {
    List<String> fields = Lists.newArrayList("Covariance");

    return fields;
  }

  @Override
  public String printTable() {
    return "%table Correlation of " + seriesX + " and " + seriesY + "\n" + covValue;
  }

  @Override
  public FetcherContext getContext() {
    this.context.setDf(DataFrameUtil.makeDataFrame(this.context.getSparkContext(), this.getFields(), this.getRows()));
    return this.context;
  }
}
