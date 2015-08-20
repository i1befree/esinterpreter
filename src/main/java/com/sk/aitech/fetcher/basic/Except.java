package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.SubCommandFetcher;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Fetcher(name="except", alias = {"minus"}, description = "except [expr]")
public class Except extends SubCommandFetcher {
  enum Element {
    EXPR
  }

  transient private static final Pattern p =
      Pattern.compile("(except|minus)\\s+\\[(?<expr>.+)\\]");

  public Except(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException {
    Matcher m = p.matcher(cmd);

    if (m.matches()) {
      this.getContext().addConfig(Element.EXPR.name(), m.group(Element.EXPR.name().toLowerCase()));
    } else
      throw new FetcherException("Invalid command : " + cmd);
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

    this.getContext().setDf(this.getContext().getDf().except(subCmdContext.getDf()));
  }
}
