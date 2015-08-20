package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 새로운 컬럼을 추가하는 명령어
 */
@Fetcher(name="add", description = "add column-name with equation")
public class Add extends DefaultFetcher{
  enum Element {
    COLUMNNAME,
    EQUATION
  }

  final static Pattern p = Pattern.compile("add (?<colname>\\w+) with (?<equation>.+)");

  public Add(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException{
    Matcher m = p.matcher(cmdLine);

    if(m.matches()){
      this.getContext().addConfig(Element.COLUMNNAME.name(), m.group("colname"));
      this.getContext().addConfig(Element.EQUATION.name(), m.group("equation"));
    }else
      throw new FetcherException("Invalid command :" + cmdLine);
  }

  @Override
  public void fetch() throws FetcherException {
    List<String> fields = this.getFields();

    fields.add(
        String.format("%s as %s"
            , this.getContext().getConfig(Element.EQUATION.name())
            , this.getContext().getConfig(Element.COLUMNNAME.name()))
    );

    this.getContext().setDf(
        this.getContext().getDf().selectExpr(fields.toArray(new String[fields.size()]))
    );
  }
}
