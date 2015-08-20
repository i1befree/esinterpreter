package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Fetcher(name="head", alias={"limit"}, description = "head number")
public class Head extends DefaultFetcher{
  static final Pattern p = Pattern.compile("(head|limit)\\s+(?<linenum>\\d+)\\s*");

  public Head(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException{
    Matcher m = p.matcher(cmd);

    if(m.matches()){
      this.getContext().addConfig("linenum", m.group("linenum").trim());
    }else
      throw new FetcherException("Invalid command : " + cmd);
  }

  @Override
  public void fetch() throws FetcherException {
    int linenum = 100;
    if(this.getContext().getConfig("linenum") != null){
      linenum = Integer.parseInt((String) this.getContext().getConfig("linenum"));
    }

    this.getContext().setDf(this.getContext().getDf().limit(linenum));
  }
}
