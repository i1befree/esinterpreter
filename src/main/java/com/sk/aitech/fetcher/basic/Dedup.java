package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Fetcher(name="dedup", alias = {"distinct"}, description = "dedup or distinct column-list")
public class Dedup extends DefaultFetcher{
  Pattern p = Pattern.compile("(dedup|distinct)\\s*(?<columns>(\\w+\\s*,?\\s*)+)?");

  public Dedup(FetcherContext context) throws FetcherException{
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException{
    Matcher m = p.matcher(cmd);

    if(m.matches()){
      if(m.group("columns") != null)
        this.getContext().addConfig("columns", m.group("columns").trim());
    }else
      throw new FetcherException("Invalid command : " + cmd);
  }

  @Override
  public void fetch() throws FetcherException {
    if(this.getContext().getConfig("columns") != null){
      List<String> dedupCols = new ArrayList<String>();
      String columns = (String)this.getContext().getConfig("columns");

      for(String column : columns.split(",")){
        dedupCols.add(column.trim());
      }

      this.getContext().setDf(this.getContext().getDf().dropDuplicates(dedupCols.toArray(new String[dedupCols.size()])));
    }else
      this.getContext().setDf(this.getContext().getDf().dropDuplicates());
  }
}
