package com.sk.aitech.fetcher.sql;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.IFetcher;
import org.durid.sql.ast.statement.SQLDeleteStatement;
import org.durid.sql.parser.SQLParserUtils;
import org.durid.sql.parser.SQLStatementParser;
import org.durid.util.JdbcUtils;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.parse.SqlParser;
import org.nlpcn.es4sql.query.DeleteQueryAction;
import org.nlpcn.es4sql.query.QueryAction;

import java.util.List;

@Fetcher(name="delete", description = "delete from source")
public class Delete implements IFetcher {
  FetcherContext context;
  QueryAction queryAction;

  public Delete(FetcherContext context) throws FetcherException{
    this.context = context;
    SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(context.getCmd(), JdbcUtils.MYSQL);
    SQLDeleteStatement deleteStatement = parser.parseDeleteStatement();
    org.nlpcn.es4sql.domain.Delete delete = null;

    try {
      delete = new SqlParser().parseDelete(deleteStatement);
      queryAction = new DeleteQueryAction(context.getConnector().getClient(), delete);
    } catch (SqlParseException e) {
      throw new FetcherException("SQLParseException : " + context.getCmd());
    }

  }

  @Override
  public void fetch() throws FetcherException {
    try {
      queryAction.explain().get();
    } catch (SqlParseException e) {
      throw new FetcherException(e);
    }
  }

  @Override
  public List<String> getRows() {
    return null;
  }

  @Override
  public List<String> getFields() {
    return null;
  }

  @Override
  public String printTable() {
    return null;
  }

  @Override
  public FetcherContext getContext() {
    return this.context;
  }
}
