package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

/**
 * 원본 정보를 기반으로 DataFrame을 생성한다.
 * 기본적으로 RDBMS, Hadoop의 경우 Lazy 하게 읽어 들일 수 있으나 ES의 경우 아직 준비되지 않았음.
 * 따라서 현재는 ES의 경우 시간 범위를 정하도록 할 예정이다. 추후 Lazy 하게 읽는 등의 작업이 필요할 것으로 보인다.
 */
@Fetcher(name="source", description = "source sourcename")
public class Source extends DefaultFetcher{
  private String sourceName;

  public Source(FetcherContext context) throws FetcherException{
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException{
    String[] items = cmdLine.split("\\s+");

    if(items.length >= 2){
      this.sourceName = items[1].trim();
    }else
      throw new FetcherException("Invalid command");
  }

  @Override
  public void fetch() throws FetcherException {
    this.context.setDf(JavaEsSparkSQL.esDF(this.context.getSparkContext(), this.sourceName));
  }
}
