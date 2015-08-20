package com.sk.aitech.fetcher.ml;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Fetcher(name="naivebayes", description = "naivebayes label=field1, features={field2,field3...}, lambda=1.0, modelType=multinomial|bernoulli")
public class NaiveBayes extends DefaultFetcher {
  final static Pattern p1 = Pattern.compile("naivebayes(?<params>((\\s*\\w+\\s*=\\s*[\\w|\\d|\\.]+\\s*,?)|(?<features>\\s*\\w+\\s*=\\s*\\{(\\s*\\w+\\s*,?\\s*)*}*\\s*,?\\s*))+)");
  final static Pattern p2 = Pattern.compile("((\\s*\\w+\\s*=\\s*\\w+)|(\\s*\\w+\\s*=\\s*\\{(\\s*\\w+\\s*,?\\s*)*}*))");

  private DataFrame expected;

  public NaiveBayes(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException {
    Matcher m1 = p1.matcher(cmdLine);

    if(m1.matches()){
      int start = 0;
      String params = m1.group("params");
      Matcher m2 = p2.matcher(params);

      while(m2.find(start)){
        int matchStart = m2.start();
        int matchEnd = m2.end();
        start = matchEnd;

        String param = params.substring(matchStart, matchEnd);
        String[] keyValue = param.split("=");
        this.getContext().addConfig(keyValue[0].trim().toLowerCase(), keyValue[1].trim());
      }
    }else
      throw new FetcherException("Invalid command");
  }

  @Override
  public void fetch() throws FetcherException {
    String labelField = (String)this.getContext().getConfig("labelfield");
    String featureFields = (String)this.getContext().getConfig("features");

    featureFields = featureFields.trim().replaceAll("\\{", "").replaceAll("\\}", "");
    List<String> featureFieldList = new ArrayList<>();

    for(String featureField : featureFields.split(","))
      featureFieldList.add(featureField.trim());

    NaiveBayesTrainer trainer = new NaiveBayesTrainer(
        this.getContext().getDf(),
        labelField,
        featureFieldList.toArray(new String[featureFieldList.size()]),
        this.getContext().getConfig()
    );

    trainer.train();

    this.expected = trainer.predict(this.getContext().getDf(), this.getContext().getSparkContext());
  }



  @Override
  public List<String> getRows() {
    if(this.context.getDf() != null && this.expected != null){
      Row[] rows = this.context.getDf().collect();
      Row[] caculatedRows = this.expected.collect();

      List<String> rowString = new ArrayList<>(rows.length);

      for(int i = 0 ; i < rows.length ; i++){
        rowString.add(rows[i].mkString("\t") + "\t" + caculatedRows[i].getDouble(1));
      }

      return rowString;
    }else{
      return null;
    }
  }

  @Override
  public List<String> getFields() {
    if(this.context.getDf() != null){
      String[] columns = this.context.getDf().columns();
      List<String> fields = new ArrayList<>(columns.length);

      for(String column: columns)
        fields.add(column);

      fields.add("expected");

      return fields;
    }else{
      return null;
    }
  }
}
