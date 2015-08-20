package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by i1befree on 15. 7. 24..
 */
@Fetcher(name="filter", description = "filter <condition>")
public class Filter extends DefaultFetcher{
  enum Element{
    CONDITION
  }

  transient Pattern p = Pattern.compile("filter (?<condition>.+)");

  public Filter(FetcherContext context) throws FetcherException{
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException{
    Matcher m = p.matcher(cmd);

    if(m.matches()){
      this.getContext().addConfig(Element.CONDITION.name(), m.group("condition"));
    }else
      throw new FetcherException("Invalid command : " + cmd);
  }

  @Override
  public void fetch() throws FetcherException {
    this.getContext().setDf(
        this.getContext().getDf().filter(String.valueOf(this.getContext().getConfig(Element.CONDITION.name())))
    );
  }
}
