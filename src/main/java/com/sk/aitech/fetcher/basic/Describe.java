package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.util.DataFrameUtil;
import org.apache.spark.sql.types.StructField;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 데이터 소스의 스키마에 대해 알려준다.
 */
@Fetcher(name="describe", alias = {"desc"}, description = "desc|describe sourcename")
public class Describe extends DefaultFetcher{
  private String sourceName;

  public Describe(FetcherContext context) throws FetcherException {
    super(context);
    parseCommand(context.getCmd());
  }

  private void parseCommand(String cmdLine) throws FetcherException{
    String[] items = cmdLine.split("\\s+");

    if(items.length >= 2){
      this.sourceName = items[1].trim();
    }else
      throw new FetcherException("Invalid command");
  }

  @Override
  public void fetch() throws FetcherException {
    //현재는 ES에 존재하는 소스에 대해서만 처리한다.
    //추후 DataSource 관리가 개발 완료 되면 여러 소스에 대해 처리할 수 있도록 변경해야 함.
    List<String> fieldNames = new ArrayList<>();
    List<String> datas = new ArrayList<>();

    Collections.addAll(fieldNames, new String[]{"FieldName", "Type", "Nullable"});
    StructField[] fields = JavaEsSparkSQL.esDF(this.context.getSparkContext(), this.sourceName).schema().fields();

    for(StructField field : fields){
      datas.add(field.name() + "\t" + field.dataType().typeName() + "\t" + (field.nullable()?"TRUE":"FALSE"));
    }

    this.getContext().setDf(DataFrameUtil.makeDataFrame(this.getContext().getSparkContext(), fieldNames, datas));
  }
}
