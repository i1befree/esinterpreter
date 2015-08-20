package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Fetcher(name="sample", description = "sample fraction=0.1, withReplacement=false, seed=1")
public class Sample extends DefaultFetcher{
  static final Pattern p = Pattern.compile("sample\\s+\\(?(?<params>(\\w+=(\\d+\\.\\d+|\\w+|\\d+)\\s*,?\\s*)*)\\)?");

  public Sample(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException{
    Matcher m = p.matcher(cmdLine);

    if(m.matches()){
      //모든 인자는 기본값을 가지고 있다.
      if(m.group("params") != null){
        String optionLine = m.group("params").trim();
        String[] options = optionLine.trim().split(",");

        for(String option : options){
          String[] optionKeyValue = option.split("=");
          this.getContext().addConfig(optionKeyValue[0].trim().toLowerCase(), optionKeyValue[1].trim());
        }
      }
    }
  }

  @Override
  public void fetch() throws FetcherException {
    double fraction = 0.1;
    boolean withReplacement = false;
    long seed;

    if(this.getContext().getConfig("fraction") != null)
      fraction = Double.parseDouble((String) this.getContext().getConfig("fraction"));

    if(this.getContext().getConfig("withreplacement") != null)
      withReplacement = Boolean.parseBoolean((String) this.getContext().getConfig("withreplacement"));

    if(this.getContext().getConfig("seed") != null)
      seed = Long.parseLong((String) this.getContext().getConfig("seed"));
    else
      seed = new Random().nextLong();

    this.getContext().setDf(this.getContext().getDf().sample(withReplacement, fraction, seed));
  }
}
