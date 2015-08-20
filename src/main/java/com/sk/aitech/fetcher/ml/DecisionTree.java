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

@Fetcher(name="decisiontree", description = "decisionTree label=field1, features={field2,field3...}, numClasses=10, impurity=gini|entropy|variance, maxDepth=5, maxBins=")
public class DecisionTree extends DefaultFetcher{
  final static Pattern p1 = Pattern.compile("naivebayes(?<params>((\\s*\\w+\\s*=\\s*[\\w|\\d|\\.]+\\s*,?)|(?<features>\\s*\\w+\\s*=\\s*\\{(\\s*\\w+\\s*,?\\s*)*}*\\s*,?\\s*))+)");
  final static Pattern p2 = Pattern.compile("((\\s*\\w+\\s*=\\s*\\w+)|(\\s*\\w+\\s*=\\s*\\{(\\s*\\w+\\s*,?\\s*)*}*))");

  private DataFrame expected;

  public DecisionTree(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException{
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


    if(labelField == null || featureFields == null)
      throw new FetcherException("labelFeild and featureFields is required");

    featureFields = featureFields.trim().replaceAll("\\{", "").replaceAll("\\}", "");
    List<String> featureFieldList = new ArrayList<>();

    for(String featureField : featureFields.split(","))
      featureFieldList.add(featureField.trim());

    DecisionTreeTrainer trainer = new DecisionTreeTrainer(
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
