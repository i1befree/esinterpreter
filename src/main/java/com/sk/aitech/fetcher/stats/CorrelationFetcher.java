package com.sk.aitech.fetcher.stats;

import com.google.common.collect.Lists;
import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.IFetcher;
import com.sk.aitech.util.DataFrameUtil;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.stat.Statistics;
import org.apache.spark.sql.DataFrame;

import java.util.List;

/**
 * 두개 필드에 대한 Correlation 값을 계산하는 Fetcher
 */
@Fetcher(name="correlation")
public class CorrelationFetcher implements IFetcher {
  private FetcherContext context;
  private String seriesX;
  private String seriesY;

  private Double correlation;

  public CorrelationFetcher(FetcherContext fetcherContext) throws FetcherException{
    context = fetcherContext;
    parseCommand(fetcherContext.getCmd());
  }

  /**
   * 명령어 해석 및 검사. 검사의 경우 추후 분리해서 가지고 갈 것
   * @param cmdLine
   * @throws FetcherException
   */
  private void parseCommand(String cmdLine) throws FetcherException{
    int cmdPos = cmdLine.indexOf(" ");
    String optionLine = cmdLine.substring(cmdPos);

    if(this.context.getDf() == null)
      throw new FetcherException("Please define source");

    if(optionLine != null && optionLine.trim().length() > 0){
      String[] options = optionLine.trim().split(",");

      if(options.length == 2){
        seriesX = options[0].trim();
        seriesY = options[1].trim();

        if(this.context.getDf().col(seriesX) == null || this.context.getDf().col(seriesY) == null)
          throw new FetcherException("Invalid column name");
      }else
        new FetcherException("You must input 2 fields");
    }
  }

  @Override
  public void fetch() throws FetcherException {
    JavaDoubleRDD seriesXRdd = DataFrameUtil.extractDoubleColumn(this.context.getDf(), seriesX);
    JavaDoubleRDD seriesYRdd = DataFrameUtil.extractDoubleColumn(this.context.getDf(), seriesY);

    this.correlation = Statistics.corr(seriesXRdd.srdd(), seriesYRdd.srdd(), "pearson");
  }

  @Override
  public List<String> getRows() {
    List<String> rows = Lists.newArrayList(correlation.toString());

    return rows;
  }

  @Override
  public List<String> getFields() {
    List<String> fields = Lists.newArrayList("Correlation");
    return fields;
  }

  @Override
  public String printTable() {
    return "%table Correlation of " + seriesX + " and " + seriesY + "\n" + correlation;
  }

  @Override
  public FetcherContext getContext() {
    this.context.setDf(DataFrameUtil.makeDataFrame(this.context.getSparkContext(), this.getFields(), this.getRows()));
    return this.context;
  }
}
