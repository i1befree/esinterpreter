package com.sk.aitech.fetcher.chart;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.ChartFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Bubble chart 용 명령어.
 */
@Fetcher(name="bubblechart", description = "bubblechart(x={xfield}, y={yfield}, z={zfield}, group={seriesfield}, size={sizefield})")
public class BubbleChart extends ChartFetcher{
  final Pattern p = Pattern.compile("(bubblechart\\s*\\()(((\\w+\\s*\\=\\s*\\w+)(\\s*,\\s*)*)+)(\\))");

  @Override
  protected Map<String, Object> getOptions() {
    return this.getContext().getConfig();
  }

  public BubbleChart(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException{
    Matcher m = p.matcher(cmdLine);

    if(m.matches() && m.groupCount() > 3){
      String optionLine = m.group(2).trim();
      String[] options = optionLine.trim().split(",");

      if(options.length >= 3){
        for(String option : options){
          String[] optionKeyValue = option.split("=");
          this.context.addConfig(optionKeyValue[0].trim().toLowerCase(), optionKeyValue[1].trim());
        }
      }else
        throw new FetcherException("You must input 3 arguments");
    }else
      throw new FetcherException("Invalid command");
  }
}
