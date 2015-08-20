package com.sk.aitech.fetcher.stats;

import com.google.common.collect.Lists;
import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.IFetcher;
import com.sk.aitech.util.DataFrameUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.DataFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by i1befree on 15. 7. 9..
 */
@Fetcher(name="summary")
public class SummaryFetcher implements IFetcher {
  final Pattern p = Pattern.compile("summary\\s*\\(?\\s*((\\w+\\s*,?\\s*)+)\\)?");

  FetcherContext context;
  String[] fields;
  List<String> fieldNames = Lists.newArrayList("Field", "Mean", "Min", "Max", "Variance", "Nonzero Count", "normL1", "normL2");
  List<String> fieldValues;

  public SummaryFetcher(FetcherContext context) throws FetcherException{
    fieldValues = new ArrayList<>();
    this.context = context;
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException{
    Matcher m = p.matcher(cmd);

    if(m.matches()){
      fields = m.group(1).split(",");
    }else
      throw new FetcherException("Invalid command");
  }

  @Override
  public void fetch() throws FetcherException {
    MultivariateStatisticalSummary summary = Statistics.colStats(DataFrameUtil.toVectorRdd(this.context.getDf(), fields).rdd());
    double[] means = summary.mean().toArray();
    double[] min = summary.min().toArray();
    double[] max = summary.max().toArray();
    double[] variance = summary.variance().toArray();
    double[] nonzero = summary.numNonzeros().toArray();
    double[] normL1 = summary.normL1().toArray();
    double[] normL2 = summary.normL2().toArray();


    for(int i = 0 ; i < fields.length ; i++){
      fieldValues.add(fields[i] + "\t" + means[i] + "\t" + min[i] + "\t" + max[i] + "\t" + variance[i] + "\t" + nonzero[i] + "\t" + normL1[i] + "\t" + normL2[i]);
    }
  }

  @Override
  public List<String> getRows() {
    return fieldValues;
  }

  @Override
  public List<String> getFields() {
    return fieldNames;
  }

  @Override
  public String printTable() {
    StringBuilder sb = new StringBuilder("%table ");

    List<String> results = this.getRows();

    for(int i = 0 ; i < this.getFields().size() ; i++){
      if(i > 0)
        sb.append("\t");

      sb.append(this.getFields().get(i));
    }

    sb.append("\n");

    for(String result : results)
      sb.append(result).append("\n");

    return sb.toString();
  }

  @Override
  public FetcherContext getContext() {
    this.context.setDf(DataFrameUtil.makeDataFrame(this.context.getSparkContext(), this.getFields(), this.getRows()));
    return this.context;
  }
}
