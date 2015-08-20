package com.sk.aitech.fetcher.basic;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.DefaultFetcher;
import com.sk.aitech.fetcher.Fetcher;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.util.LogDefiner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Source 와 크게 다르지 않지만, 무작위 적인 데이터를 읽어 들이는데 사용한다.
 * 1차 지원 대상은 http 파일 다운로드, hdfs, s3 이다.
 * 대상이 파일일 경우 자동으로 분류 정보를 찾아줄 수 있다. 하지만 대상이 디렉터리일 경우에는 샘플을 활용해서 스키마를
 * 예상해야 하며, 각 라인별로 스키마가 같은지 또는 다른지 판정해야 한다.
 * 차후에는 잘 알려진 로그에 대한 패턴을 등록하고 자동으로 인지할 수 있는 기능을 구현해야 한다.
 */
@Fetcher(name="load", description = "load uri")
public class Load extends DefaultFetcher{
  transient private Pattern p = Pattern.compile("[^\\w|\\.]+");

  private String uri;
  private String seperateStr;

  public Load(FetcherContext context) throws FetcherException{
    super(context);
    String[] items = this.context.getCmd().split("\\s+");
    uri = items[1];
  }

  @Override
  public void fetch() throws FetcherException {
    if(uri.startsWith("hdfs://") || uri.startsWith("webhdfs://") || uri.startsWith("file://")){
      JavaRDD rdd = this.context.getSparkContext().sparkContext().textFile(uri, 8).toJavaRDD();

      List<String> data = rdd.takeSample(false, 100);
      final LogDefiner definer = new LogDefiner(data);
      StructType schema = definer.getSchema();

      JavaRDD<Row> rowRDD = rdd.map(new Function<String, Row>() {
        @Override
        public Row call(String data) throws Exception {
          String[] fields = data.split(definer.getSeperateStr());
          return RowFactory.create(fields);
        }
      });

      //TODO:df 가 이미 있을 때는 어떻게 해야 하지?
      this.context.setDf(this.context.getSparkContext().createDataFrame(rowRDD, schema));
    }else if(uri.startsWith("http://")){

    }else if(uri.startsWith("")){

    }
  }
}
