package com.sk.aitech.fetcher.chart;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.ChartFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Fetcher(name="timechart", description = "timechart(key={keyfield}, group={groupfield}, value={valuefield})")
public class TimeChart extends ChartFetcher {
  final Pattern p = Pattern.compile("(timechart\\s*\\()(((\\w+\\s*\\=\\s*\\w+)(\\s*,\\s*)*)+)(\\))");

  /**
   * cmdLine like timechart(key=x, group=a, value=y)
   * @param context
   */
  public TimeChart(FetcherContext context) throws FetcherException{
    super(context);
    parseCommand(context.getCmd());
  }

  @Override
  protected Map<String, Object> getOptions() {
    return this.getContext().getConfig();
  }

  /**
   * 명령어 해석 및 검사. 검사의 경우 추후 분리해서 가지고 갈 것
   * @param cmdLine
   * @throws FetcherException
   */
  private void parseCommand(String cmdLine) throws FetcherException{
    Matcher m = p.matcher(cmdLine);

    if(m.matches() && m.groupCount() > 3){
      String optionLine = m.group(2).trim();
      String[] options = optionLine.trim().split(",");

      if(options.length >= 2){
        for(String option : options){
          String[] optionKeyValue = option.split("=");
          this.getContext().addConfig(optionKeyValue[0].trim().toLowerCase(), optionKeyValue[1].trim());
        }
      }else
        throw new FetcherException("You must input 3 arguments");
    }else
      throw new FetcherException("Invalid command");
  }
}
