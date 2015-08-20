package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 특정 필드의 값을 업데이트 한다.
 */
@Fetcher(name="update", description = "update column-name with equation")
public class Update extends DefaultFetcher{
  enum Element {
    COLUMNNAME,
    EQUATION
  }

  transient Pattern p = Pattern.compile("update (?<colname>\\w+) with (?<equation>.+)");

  public Update(FetcherContext context) throws FetcherException{
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException{
    Matcher m = p.matcher(cmdLine);

    if(m.matches()){
      this.context.addConfig(Element.COLUMNNAME.name(), m.group("colname"));
      this.context.addConfig(Element.EQUATION.name(), m.group("equation"));
    }else
      throw new FetcherException("Invalid command :" + cmdLine);
  }

  @Override
  public void fetch() throws FetcherException {
    List<String> fields = this.getFields();
    fields.remove(this.context.getConfig(Element.COLUMNNAME.name()));

    fields.add(
        String.format("%s as %s"
            , this.context.getConfig(Element.EQUATION.name())
            , this.context.getConfig(Element.COLUMNNAME.name()))
    );

    this.context.setDf(
        this.context.getDf().selectExpr(fields.toArray(new String[0]))
    );
  }
}
