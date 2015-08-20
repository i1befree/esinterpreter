package com.sk.aitech.fetcher.ml;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by i1befree on 15. 7. 9..
 */
@Fetcher(name="linear", description = "linear method=svm|logistic|linear, label=field1, features={field2,field3...}, optimization=SGD|LBF, mode=train|fit, numberOfclasses=3, numberOfIterations=10")
public class Linear extends DefaultFetcher {
  final static Pattern p1 = Pattern.compile("linear\\s*\\(?\\s*(?<params>((\\s*\\w+\\s*=\\s*\\w+\\s*,?\\s*)|(?<features>\\s*\\w+\\s*=\\s*\\{(\\s*\\w+\\s*,?\\s*)*}*\\s*,?\\s*))+)|");
  final static Pattern p2 = Pattern.compile("((\\s*\\w+\\s*=\\s*\\w+)|(\\s*\\w+\\s*=\\s*\\{(\\s*\\w+\\s*,?\\s*)*}*))");

  transient DataFrame expected;

  String method;
  String label;
  String[] features;
  String mode;
  Map<String, String> options;

  public Linear(FetcherContext context) throws FetcherException{
    super(context);
    options = new HashMap<>();

    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException{
    Matcher m1 = p1.matcher(cmdLine);

    if(m1.matches()){
      String params = m1.group("params");
      Matcher m2 = p2.matcher(params);
      int start = 0;

      while(m2.find(start)){
        int matchStart = m2.start();
        int matchEnd = m2.end();
        start = matchEnd;

        String param = params.substring(matchStart, matchEnd);
        String[] keyValue = param.split("=");

        switch (keyValue[0].trim().toUpperCase()){
          case "METHOD" :
            this.method = keyValue[1].trim();
            break;
          case "LABEL" :
            this.label = keyValue[1].trim();
            break;
          case "FEATURES" :
            String replacedField = keyValue[1].trim().replaceAll("\\{", "").replaceAll("\\}", "");
            String[] featuredfields = replacedField.split(",");
            this.features = new String[featuredfields.length];

            for(int i = 0 ; i < featuredfields.length ; i++)
              this.features[i] = featuredfields[i].trim();

            break;
          case "OPTIMIZATION" :
            options.put("optimization", keyValue[1].trim());
            break;
          case "MODE" :
            this.mode = keyValue[1].trim();
            break;
          case "NUMBEROFCLASSES" :
            options.put("numberOfClasses", keyValue[1].trim());
            break;
          case "NUMBEROFITERATIONS" :
            options.put("numberOfIterations", keyValue[1].trim());
            break;
        }
      }
    }else{
      throw new FetcherException("Invalid command");
    }
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

  @Override
  public void fetch() throws FetcherException {
    LinearModelTrainer trainer = new LinearModelTrainer(this.context.getDf(), this.method, this.label, this.features, this.options);
    trainer.train();
    this.expected = trainer.predict(this.context.getDf(), this.context.getSparkContext());
  }
}
