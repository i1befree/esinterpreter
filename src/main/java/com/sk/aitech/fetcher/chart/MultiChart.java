package com.sk.aitech.fetcher.chart;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.ChartFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Fetcher(name = "multichart", description = "multichart(key=age,value={bar:userCnt, time:[age,gender]})")
public class MultiChart extends ChartFetcher {
  static Pattern p = Pattern.compile("multichart\\s*\\(?\\s*(?<params>((key\\s*=\\s*\\w+|value\\s*=\\s*\\{.+\\})\\s*,?\\s*)+)\\s*\\)?", Pattern.CASE_INSENSITIVE);
  static Pattern p1 = Pattern.compile("((?<keyvalue>key\\s*=\\s*\\w+|value\\s*=\\s*\\{.+\\})\\s*,?\\s*)", Pattern.CASE_INSENSITIVE);
  static Pattern p2 = Pattern.compile("(bar\\s*:\\s*(?<field>\\w+)|time\\s*:\\s*\\[(?<fields>(\\s*\\w+\\s*,?\\s*)+)\\])", Pattern.CASE_INSENSITIVE);

  public MultiChart(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  @Override
  protected Map<String, Object> getOptions() {
    return this.getContext().getConfig();
  }

  private void parseCommand(String cmd) throws FetcherException {
    Matcher m = p.matcher(cmd);

    if (m.matches()) {
      int start = 0;
      String params = m.group("params");
      Matcher m1 = p1.matcher(params);

      while(m1.find(start)){
        String keyValuePair = m1.group("keyvalue");
        String[] keyValuePairs = keyValuePair.split("=");

        if(keyValuePairs[0].trim().toLowerCase().equals("key")){
          this.getContext().addConfig(keyValuePairs[0].trim().toLowerCase(), keyValuePairs[1].trim());
        }else if(keyValuePairs[0].trim().toLowerCase().equals("value")){
          Map<String, Object> chartMap = new HashMap<>();
          String chartStr = keyValuePairs[1].trim();
          Matcher m2 = p2.matcher(chartStr);
          int subStart = 0;

          while(m2.find(subStart)){
            String[] chartPair = chartStr.substring(m2.start(), m2.end()).split(":");
            String chartType = chartPair[0].trim().toLowerCase();

            if(chartType.equals("bar")){
              chartMap.put(chartType, chartPair[1].trim());
            }else if(chartType.equals("time")){
              chartMap.put(chartType, m2.group("fields").split("\\s*,\\s*"));
            }

            subStart = m2.end();
          }

          this.getContext().addConfig("value", chartMap);
        }

        start = m1.end();
      }
    } else
      throw new FetcherException("Invalid command");
  }
}
