package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import org.apache.spark.sql.Column;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Projection 용 필드
 */
@Fetcher(name="fields", alias = {"columns"}, description = "fields column-list")
public class Fields extends DefaultFetcher {
  final static Pattern p = Pattern.compile("(fields|columns)\\s+(?<columns>(\\w+\\s*,?\\s*)+)", Pattern.CASE_INSENSITIVE);

  public Fields(FetcherContext context) throws FetcherException{
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException{
    Matcher m = p.matcher(cmd);

    if(m.matches()){
      this.getContext().addConfig("columns", m.group("columns"));
    }else
      throw new FetcherException("Invalid command : " + cmd);
  }

  @Override
  public void fetch() throws FetcherException {
    List<Column> cols = new ArrayList<>();
    String columns = (String)this.getContext().getConfig("columns");

    for(String column : columns.split(",")){
      cols.add(this.getContext().getDf().col(column.trim()));
    }

    this.getContext().setDf(this.getContext().getDf().select(cols.toArray(new Column[cols.size()])));
  }
}
