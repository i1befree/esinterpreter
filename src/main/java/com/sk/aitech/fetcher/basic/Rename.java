package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by i1befree on 15. 7. 22..
 */
@Fetcher(name="rename", description = "rename {fieldname1, fieldname2...} to {newname1, newname2...}")
public class Rename extends DefaultFetcher{
  Pattern p = Pattern.compile("rename\\s*\\{?(?<oldname>(\\w+\\s*,?\\s*)+)\\}?\\s*to\\s*\\{?(?<newname>(\\w+\\s*,?\\s*)+)\\}?\\s*");

  private String[] oldNames;
  private String[] newNames;

  public Rename(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException {
    Matcher m = p.matcher(cmdLine);

    if(m.matches()){
      oldNames = m.group("oldname").split(",");
      newNames = m.group("newname").split(",");
    }
  }

  @Override
  public void fetch() throws FetcherException {
    if(oldNames.length == newNames.length){
      for(int i = 0 ; i < oldNames.length ; i++){
        if(this.context.getDf().col(oldNames[i].trim()) != null){
          this.context.setDf(this.context.getDf().withColumnRenamed(oldNames[i].trim(), newNames[i].trim()));
        }else
          throw new FetcherException("Fieldname [" + oldNames[i] + "] doesn't exsist");
      }
    }else{
      throw new FetcherException("The count of source field must be same with count of target");
    }
  }
}
