package com.sk.aitech.fetcher;

import com.sk.aitech.exceptions.FetcherException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 기본 Fetch용 Fetcher로 출력은 table 포맷으로, 그외 스키마는 DataFrame을 이용해 출력한다.
 * DefaultFetcher의 fetch 메소드는 호출 시 실제 액션이 실행은 바로 되지 않으며 Plan 만을 생성하게 된다.
 */
public abstract class DefaultFetcher implements IFetcher{
  transient protected FetcherContext context;

  public DefaultFetcher(FetcherContext context) throws FetcherException{
    this.context = context;
  }

  @Override
  public abstract void fetch() throws FetcherException;

  @Override
  public List<String> getRows() {
    return this.context.getDf().toJavaRDD().map(new Function<Row, String>() {
      @Override
      public String call(Row row) throws Exception {
        return row.mkString("\t");
      }
    }).take(
        Integer.parseInt(this.context.getEnvProperties().getProperty("zeppelin.elasticsearch.maxResult"))
    );
  }

  @Override
  public List<String> getFields() {
    List<String> fields = new ArrayList<>();
    Collections.addAll(fields, this.context.getDf().columns());

    return fields;
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
    return this.context;
  }
}
