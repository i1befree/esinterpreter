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
 * Created by i1befree on 15. 7. 28..
 */
@Fetcher(name="sort", description = "sort (-)column1, column2...")
public class Sort extends DefaultFetcher{
  transient Pattern p = Pattern.compile("sort (?<columns>(\\s*-?\\w+\\s*,?\\s*)+)");
  private String columns;

  public Sort(FetcherContext context) throws FetcherException{
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException{
    Matcher m = p.matcher(cmd);

    if(m.matches()){
      this.columns = m.group("columns");
    }else
      throw new FetcherException("Invalid command");
  }

  @Override
  public void fetch() throws FetcherException {
    String[] items = this.columns.split(",");
    List<Column> orderColumns = new ArrayList<>(items.length);

    for(String item : items){
      String colName;
      boolean isDesc = false;

      if(item.trim().startsWith("-")){
        isDesc = true;
        colName = item.trim().substring(1);
      }else
        colName = item.trim();


      if(this.context.getDf().col(colName) != null){
        if(isDesc)
          orderColumns.add(this.context.getDf().col(colName).desc());
        else
          orderColumns.add(this.context.getDf().col(colName).asc());
      }else
        throw new FetcherException("Invalid column name : " + colName);
    }

    this.context.setDf(this.context.getDf().sort(orderColumns.toArray(new Column[0])));
  }
}
