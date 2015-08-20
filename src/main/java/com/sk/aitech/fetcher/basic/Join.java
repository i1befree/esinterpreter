package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.SubCommandFetcher;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Fetcher(name = "join", description = "join type=inner|left|right column1,column2... [source=other|...]")
public class Join extends SubCommandFetcher {
  enum Element {
    JOINTYPE,
    JOINCOL,
    EXPR
  }

  enum JOINTYPE {
    INNER,
    LEFT,
    RIGHT,
    OUTER
  }

  transient private static final Pattern p =
      Pattern.compile("join\\s+(?<jointype>type\\s*=\\s*(inner|left|right))?\\s*(?<joincol>\\w+\\s*,?\\s*)\\s*\\[(?<expr>.+)\\]");

  public Join(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException {
    Matcher m = p.matcher(cmd);

    if (m.matches()) {
      defineJoinType(m.group(Element.JOINTYPE.name().toLowerCase()));
      this.getContext().addConfig(Element.JOINCOL.name(), m.group(Element.JOINCOL.name().toLowerCase()));
      this.getContext().addConfig(Element.EXPR.name(), m.group(Element.EXPR.name().toLowerCase()));
    } else
      throw new FetcherException("Invalid command : " + cmd);
  }

  private void defineJoinType(String expr) {
    if (expr == null)
      this.getContext().addConfig(Element.JOINTYPE.name(), JOINTYPE.INNER);
    else {
      if (expr.toUpperCase().indexOf(JOINTYPE.INNER.name()) > 0)
        this.getContext().addConfig(Element.JOINTYPE.name(), JOINTYPE.INNER);
      else if (expr.toUpperCase().indexOf(JOINTYPE.LEFT.name()) > 0)
        this.getContext().addConfig(Element.JOINTYPE.name(), JOINTYPE.LEFT);
      else if (expr.toUpperCase().indexOf(JOINTYPE.RIGHT.name()) > 0)
        this.getContext().addConfig(Element.JOINTYPE.name(), JOINTYPE.RIGHT);
      else if (expr.toUpperCase().indexOf(JOINTYPE.OUTER.name()) > 0)
        this.getContext().addConfig(Element.JOINTYPE.name(), JOINTYPE.OUTER);
    }
  }

  @Override
  public void fetch() throws FetcherException {
    //SubCmd부터 먼저 처리.
    FetcherContext subCmdContext = fetchSubCmd(
        (String) this.getContext().getConfig(Element.EXPR.name()),
        new FetcherContext(
            this.getContext().getEnvProperties(),
            this.getContext().getSparkContext(),
            this.getContext().getConnector(),
            null, null
        )
    );

    //TODO: Join 시 추후 source relationship 이 구현되 있으면 자동으로 이어 주는 기능 제공해야함.
    String joinCols = (String)this.getContext().getConfig(Element.JOINCOL.name());
    Column joinExpr = null;

    for(String joinCol : joinCols.split(",")){
      if(joinExpr == null)
        joinExpr = this.getContext().getDf().col(joinCol).equalTo(subCmdContext.getDf().col(joinCol));
      else
        joinExpr = joinExpr.and(this.getContext().getDf().col(joinCol).equalTo(subCmdContext.getDf().col(joinCol)));
    }

    String joinType;

    switch ((JOINTYPE)this.getContext().getConfig(Element.JOINTYPE.name())){
      case RIGHT:
        joinType = "right_outer";
        break;
      case LEFT:
        joinType = "left_outer";
        break;
      case OUTER:
        joinType = "outer";
        break;
      default:
        joinType = "inner";
    }

    DataFrame joinDf = this.getContext().getDf().join(subCmdContext.getDf(),
        joinExpr,
        joinType
    );

    this.getContext().setDf(joinDf);
  }
}
