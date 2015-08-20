package com.sk.aitech.fetcher;

import com.google.gson.Gson;
import com.sk.aitech.exceptions.FetcherException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;

/**
 * Chart fetch용 fetcher
 */
public class ChartFetcher extends DefaultFetcher{
  public ChartFetcher(FetcherContext context) throws FetcherException{
    super(context);
  }

  protected Map<String, Object> getOptions() {
    return this.getContext().getConfig();
  }

  protected String getChartName(){
    Annotation[] annotations = this.getClass().getAnnotations();

    for(Annotation annotation : annotations){
      if(annotation instanceof Fetcher)
        return ((Fetcher)annotation).name();
    }

    return null;
  }

  @Override
  public void fetch() throws FetcherException {
    //Nothing to do..
  }

  @Override
  public String printTable() {
    StringBuilder sb = new StringBuilder("%chart ");

    Map<String, Object> map = new HashMap<>();

    map.put("type", this.getChartName());
    map.put("options", getOptions());

    map.put("datas", this.context.getDf().toJavaRDD().map(new Function<Row, Map<String, String>>() {
      @Override
      public Map<String, String> call(Row row) throws Exception {
        StructField[] fields = row.schema().fields();
        Map<String, String> map = new HashMap<>();

        //TODO: Type 에 대한 정의를 하고 가야 함.
        for (int i = 0 ; i < fields.length ; i++) {
          if(fields[i].dataType() instanceof StringType)
            map.put(fields[i].name(), row.getString(i));
          else if(fields[i].dataType() instanceof LongType)
            map.put(fields[i].name(), String.valueOf(row.getLong(i)));
          else if(fields[i].dataType() instanceof DoubleType)
            map.put(fields[i].name(), String.valueOf(row.getDouble(i)));
          else if(fields[i].dataType() instanceof IntegerType)
            map.put(fields[i].name(), String.valueOf(row.getInt(i)));
          else if(fields[i].dataType() instanceof DateType)
            map.put(fields[i].name(), String.valueOf(row.getDate(i)));
        }

        return map;
      }
    }).collect());

    Gson gson = new Gson();
    String json = gson.toJson(map);

    sb.append(json);

    return sb.toString();
  }
}
