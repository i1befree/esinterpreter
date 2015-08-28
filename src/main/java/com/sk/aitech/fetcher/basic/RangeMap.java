package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by i1befree on 15. 8. 27..
 */
@Fetcher(name = "rangemap", description = "rangemap field=<string>(<attribute_name>=<numeric_range>)...(default=<sring>)")
public class RangeMap extends DefaultFetcher {
  Pattern p = Pattern.compile("rangemap\\s+(?<keyvalue>(.+\\s*=\\s*.+\\s*)+)", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.UNICODE_CHARACTER_CLASS);
  Pattern keyValueP = Pattern.compile("[^\\s=]+\\s*=\\s*[^\\s=]+", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.UNICODE_CHARACTER_CLASS);

  public RangeMap(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(this.getContext().getCmd());
  }

  private void parseCommand(String cmd) throws FetcherException {
    Matcher m = p.matcher(cmd);

    if (m.matches()) {
      int start = 0;
      List<Range> ranges = new ArrayList<>();
      String keyValues = m.group("keyvalue");
      Matcher m2 = keyValueP.matcher(keyValues);


      while (m2.find(start)) {
        String keyValue = keyValues.substring(m2.start(), m2.end()).trim();

        String[] keyValuePair = keyValue.split("\\s*=\\s*");

        if ("field".equals(keyValuePair[0].trim().toLowerCase()) || "default".equals(keyValuePair[0].trim().toLowerCase()))
          this.getContext().addConfig(keyValuePair[0].trim().toLowerCase(), keyValuePair[1]);
        else {
          String[] rangeValues = keyValuePair[1].split("\\s*-\\s*");
          Range range = new Range(
              keyValuePair[0].trim(),
              Double.parseDouble(rangeValues[0].trim()),
              Double.parseDouble(rangeValues[1].trim())
          );

          ranges.add(range);
        }

        start = m2.end();
      }

      this.getContext().addConfig("ranges", ranges);
    } else
      throw new FetcherException("Invalid command");
  }

  @Override
  public void fetch() throws FetcherException {
    final String fieldName = (String) this.getContext().getConfig("field");
    final String defaultRange = (String) this.getContext().getConfig("default");
    final List<Range> ranges = (List<Range>) this.getContext().getConfig("ranges");

    StructField[] fields = this.getContext().getDf().schema().fields();
    List<StructField> fieldList = new ArrayList<>();
    Collections.addAll(fieldList, fields);
    fieldList.add(DataTypes.createStructField("range", DataTypes.StringType, true));
    StructType schema = DataTypes.createStructType(fieldList);

    JavaRDD<Row> rowRDD = this.getContext().getDf().toJavaRDD().map(new Function<Row, Row>() {
      @Override
      public Row call(Row row) throws Exception {
        String value = defaultRange;
        Object fieldValue = row.get(row.fieldIndex(fieldName));

        for (Range range : ranges) {
          if (fieldValue instanceof Integer && range.include((Integer) fieldValue)) {
            value = range.getValue();
          } else if (fieldValue instanceof Double && range.include((Double) fieldValue)) {
            value = range.getValue();
          } else if (fieldValue instanceof String && range.include(Double.parseDouble((String)fieldValue))) {
            value = range.getValue();
          }
        }

        List<Object> datas = new ArrayList<>();

        for (int i = 0; i < row.length(); i++)
          datas.add(row.get(i));

        datas.add(value);

        return RowFactory.create(datas.toArray());
      }
    });

    DataFrame dataFrame = this.getContext().getSparkContext().createDataFrame(rowRDD, schema);
    this.getContext().setDf(dataFrame);
  }

  /**
   * 문자열 등과도 비교를 처리해야 하는지 판단.
   */
  private static class Range implements Serializable {
    double startValue;
    double endValue;
    String value;

    public Range(String value, double startValue, double endValue) {
      this.value = value;
      this.startValue = startValue;
      this.endValue = endValue;
    }

    public boolean include(int num) {
      return (num > startValue) && (num < endValue);
    }

    public boolean include(double num) {
      return (num > startValue) && (num < endValue);
    }

    public double getStartValue() {
      return startValue;
    }

    public double getEndValue() {
      return endValue;
    }

    public String getValue() {
      return value;
    }
  }
}
