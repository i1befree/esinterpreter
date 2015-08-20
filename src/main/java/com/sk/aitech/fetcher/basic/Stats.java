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

import static org.apache.spark.sql.functions.*;

@Fetcher(name = "stats", description = "stats stat_func(columnname) [as field] by column list")
public class Stats extends DefaultFetcher {
  enum Element {
    STATFUNC,
    AGGCOL
  }

  enum STATFUNC {
    MIN,
    MAX,
    AVG,
    COUNT,
    SUM,
    SUMDISTINCT,
    APPROAXCOUNTDISTINCT,
    COUNTDISTINCT,
    FIRST,
    LAST,
    STDEV,
    STDEVP,
    VAR
  }

  Pattern p = Pattern.compile("stats\\s+(?<statfunc>(\\w+\\s*\\(\\s*.+\\s*\\)\\s*(\\s+as\\s+\\w+)?,?)+)\\s+by\\s+(?<aggcol>(\\w+\\s*,?\\s*)+)");
  Pattern funcP = Pattern.compile("(?<funcname>\\w+)\\s*\\((?<colname>\\s*.+\\s*)\\)\\s*(\\s+as\\s+(?<alias>\\w+))?");

  public Stats(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException {
    Matcher m = p.matcher(cmd);

    if (m.matches()) {
      this.getContext().addConfig(Element.STATFUNC.name(), m.group(Element.STATFUNC.name().toLowerCase()));
      this.getContext().addConfig(Element.AGGCOL.name(), m.group(Element.AGGCOL.name().toLowerCase()));
    } else
      throw new FetcherException("Invalid command : " + cmd);
  }

  @Override
  public void fetch() throws FetcherException {
    String aggCols = (String) this.getContext().getConfig(Element.AGGCOL.name());
    List<Column> cols = new ArrayList<>();

    for (String aggCol : aggCols.split(",")) {
      cols.add(this.getContext().getDf().col(aggCol.trim()));
    }

    List<Column> statsCol = new ArrayList<>();
    String statFuncs = (String) this.getContext().getConfig(Element.STATFUNC.name());

    for (String statFunc : statFuncs.split(","))
      statsCol.add(defineStatfunc(statFunc.trim()));

    Column firstCol = statsCol.remove(0);
    this.getContext().setDf(
        this.getContext().getDf()
            .groupBy(cols.toArray(new Column[0]))
            .agg(firstCol, statsCol.toArray(new Column[0]))
    );
  }

  /**
   * 집계용 함수를 반환한다. 현재는 min, max, sum, avg, count 만을 지원한다.
   * @param statFunc
   * @return
   */
  private Column defineStatfunc(String statFunc) {
    Column statCol = null;
    Matcher m = funcP.matcher(statFunc);

    if(m.matches()){
      String functionName = m.group("funcname").toUpperCase();
      //TODO:Multi-column 함수로 전환을 고려할 것. approxmate 함수는 인자가 들어오는데 대책 세울 것.
      String columnName = m.group("colname");

      if(functionName.equals(STATFUNC.AVG.name()))
        statCol = avg(columnName);
      else if(functionName.equals(STATFUNC.MAX.name()))
        statCol = max(columnName);
      else if(functionName.equals(STATFUNC.MIN.name()))
        statCol = min(columnName);
      else if(functionName.equals(STATFUNC.SUM.name()))
        statCol = sum(columnName);
      else if(functionName.equals(STATFUNC.COUNT.name()))
        statCol = count(columnName);
      else if(functionName.equals(STATFUNC.FIRST.name()))
        statCol = first(columnName);
      else if(functionName.equals(STATFUNC.LAST.name()))
        statCol = last(columnName);
      else if(functionName.equals(STATFUNC.SUMDISTINCT.name()))
        statCol = last(columnName);
      else if(functionName.equals(STATFUNC.APPROAXCOUNTDISTINCT.name()))
        statCol = last(columnName);
      else if(functionName.equals(STATFUNC.COUNTDISTINCT.name()))
        statCol = last(columnName);

      if(statCol != null && m.group("alias") != null){
        statCol = statCol.as(m.group("alias"));
      }
    }

    return statCol;
  }
}
