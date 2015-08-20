package com.sk.aitech.fetcher.sql;

import com.sk.aitech.elasticsearch.ESConnector;
import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.IFetcher;
import org.durid.sql.SQLUtils;
import org.durid.sql.ast.expr.SQLQueryExpr;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.SqlParser;

import java.util.List;

/**
 * Created by i1befree on 15. 7. 27..
 */
@Fetcher(name="select", description="select ...")
public class Select implements IFetcher{
  private IFetcher fetcher;

  public Select(FetcherContext context) throws FetcherException{
    ESConnector connector = context.getConnector();
    SQLQueryExpr sqlExpr = (SQLQueryExpr) SQLUtils.toMySqlExpr(context.getCmd());
    org.nlpcn.es4sql.domain.Select select;

    try {
      select = new SqlParser().parseSelect(sqlExpr);
      if (select.isAgg) {
        this.fetcher = new AggregationQueryFetcher(context, select);
      } else {
        this.fetcher = new DefaultQueryFetcher(context, select);
      }
    } catch (SqlParseException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void fetch() throws FetcherException {
    this.fetcher.fetch();
  }

  @Override
  public List<String> getRows() {
    return this.fetcher.getRows();
  }

  @Override
  public List<String> getFields() {
    return this.fetcher.getFields();
  }

  @Override
  public String printTable() {
    return this.fetcher.printTable();
  }

  @Override
  public FetcherContext getContext() {
    return this.fetcher.getContext();
  }
}
