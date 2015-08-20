package com.sk.aitech.fetcher.sql;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.IFetcher;
import com.sk.aitech.util.DataFrameUtil;
import org.apache.spark.sql.DataFrame;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.nlpcn.es4sql.domain.Field;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.DefaultQueryAction;
import org.nlpcn.es4sql.query.QueryAction;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by i1befree on 15. 6. 29..
 */
public class DefaultQueryFetcher implements IFetcher {
  private DataFrame df;
  private List<String> rows;
  private List<String> columnNames;
  private List<String> originalColumnName;
  private List<Field> fields;
  private FetcherContext context;
  private QueryAction queryAction;

  public DefaultQueryFetcher(FetcherContext context, org.nlpcn.es4sql.domain.Select select){
    this.context = context;
    this.queryAction = new DefaultQueryAction(this.context.getConnector().getClient(), select);
    rows = new ArrayList<>();
    columnNames = new ArrayList<>();
    originalColumnName = new ArrayList<>();
    fields = new ArrayList<>();
  }

  @Override
  public void fetch() throws FetcherException {
    int limitSize = Integer.parseInt(this.context.getEnvProperties().getProperty("zeppelin.elasticsearch.maxResult"));
    SearchRequestBuilder requestBuilder = null;

    try {
      requestBuilder = (SearchRequestBuilder)queryAction.explain();
      requestBuilder.setSize(limitSize);
    } catch (SqlParseException e) {
      throw new FetcherException(e);
    }

    SearchHits hits = requestBuilder.get().getHits();

    for(int i = 0 ; i < hits.getTotalHits() && i < limitSize ; i++){
      StringBuilder sb = new StringBuilder();
      SearchHit hit = hits.getAt(i);

      //projection이 존재하는 경우 projection 순서대로 데이터를 가져오기
      if(i == 0 && fields.size() > 0){
        for(int fieldIdx = 0 ; fieldIdx < fields.size() ; fieldIdx++){
          if(fields.get(fieldIdx).getAlias() != null && fields.get(fieldIdx).getAlias().length() > 0)
            columnNames.add(fields.get(fieldIdx).getAlias());
          else
            columnNames.add(fields.get(fieldIdx).getName());

          originalColumnName.add(fields.get(fieldIdx).getName());
        }
        //projection 이 존재하지 않는 경우, 예를 들어 * 와 같은 경우, 추후에는 스키마를 읽어들여서 가져올 것.
      }else if(i == 0 && fields.size() == 0){
        String[] allFields = hit.getSource().keySet().toArray(new String[0]);

        for(int fieldIdx = 0 ; fieldIdx < allFields.length ; fieldIdx++) {
          columnNames.add(allFields[fieldIdx]);
          originalColumnName.add(allFields[fieldIdx]);
        }
      }

      for(int fieldIdx = 0 ; fieldIdx < originalColumnName.size() ; fieldIdx++){
        if(fieldIdx > 0)
          sb.append("\t");

        sb.append(hit.getSource().get(originalColumnName.get(fieldIdx)));
      }

      rows.add(sb.toString());
    }
  }

  @Override
  public List<String> getRows() {
    return rows;
  }

  @Override
  public List<String> getFields() {
    return columnNames;
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
    if(this.context != null){
      this.context.setDf(DataFrameUtil.makeDataFrame(this.context.getSparkContext(), this.getFields(), this.getRows()));
    }

    return this.context;
  }
}
