package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.SubCommandFetcher;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SQL의 Union all 과 유사한 기능 제공. 필드에 대한 매핑은 자동으로 수행한다.
 */
@Fetcher(name = "append", description = "append [expr]")
public class Append extends SubCommandFetcher {
  enum Element {
    EXPR
  }

  final static Pattern p = Pattern.compile("append\\s+\\[(?<expr>.+)\\]");

  public Append(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException {
    Matcher m = p.matcher(cmd);

    if(m.matches()){
      this.getContext().addConfig(Element.EXPR.name(), m.group(Element.EXPR.name().toLowerCase()));
    }
  }

  @Override
  public void fetch() throws FetcherException {
    FetcherContext subContext = fetchSubCmd((String)this.getContext().getConfig(Element.EXPR.name()),
        new FetcherContext(
            this.getContext().getEnvProperties(),
            this.getContext().getSparkContext(),
            this.getContext().getConnector(),
            null, null
        )
    );

    StructField[] mergedFields = this.mergeField(
        this.getContext().getDf().schema().fields(), subContext.getDf().schema().fields()
    );

    List<String> cols = new ArrayList<>(mergedFields.length);

    for(StructField field : mergedFields){
      if(this.getContext().getDf().schema().contains(field))
        cols.add(field.name());
      else {
        cols.add("NULL AS " + field.name());
      }
    }

    DataFrame left = this.getContext().getDf().selectExpr(cols.toArray(new String[0]));

    cols = new ArrayList<>(mergedFields.length);

    for(StructField field : mergedFields){
      if(subContext.getDf().schema().contains(field))
        cols.add(field.name());
      else {
        cols.add("NULL AS " + field.name());
      }
    }

    DataFrame right = subContext.getDf().selectExpr(cols.toArray(new String[0]));

    this.getContext().setDf(left.unionAll(right));
  }
}
