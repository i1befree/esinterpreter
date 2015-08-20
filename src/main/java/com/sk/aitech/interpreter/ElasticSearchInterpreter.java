package com.sk.aitech.interpreter;

import com.sk.aitech.elasticsearch.ESConnector;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.FetcherFactory;
import com.sk.aitech.fetcher.IFetcher;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;

import java.util.List;
import java.util.Properties;

/**
 * ElasticSearch 를 활용한 인터프리터
 * | 를 구분자로 연속적으로 명령어를 수행할 수 있도록 구현됨.
 * 각 명령어간 데이터는 Spark 의 DataFrame을 이용해 공유한다.
 */
public class ElasticSearchInterpreter extends Interpreter {
//  Logger logger = LoggerFactory.getLogger(ElasticSearchInterpreter.class);
  private transient ESConnector connector;
  private transient SQLContext sc;

  static {
    Interpreter.register(
        "elasticsearch",
        "elasticsearch",
        ElasticSearchInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
            .add("server.list",
                getSystemDefault("ES_ADDRESS", "server.list", "cep1:9300"),
                "ElasticSearch master uries. ex) host1:9099,host2:9099")
            .add("cluster.name",
                getSystemDefault("ES_ADDRESS", "cluster.name", "default"),
                "ElasticSearch cluster name. ex) default")
            .add("zeppelin.elasticsearch.maxResult",
                getSystemDefault("ZEPPELIN_ELASTICSEARCH_MAXRESULT", "zeppelin.elasticsearch.maxResult", "10000"),
                "Max number of Elasticsearch result to display.")
            .add("args", "", "spark commandline args").build());
  }

  public static String getSystemDefault(
      String envName,
      String propertyName,
      String defaultValue) {

    if (envName != null && !envName.isEmpty()) {
      String envValue = System.getenv().get(envName);
      if (envValue != null) {
        return envValue;
      }
    }

    if (propertyName != null && !propertyName.isEmpty()) {
      String propValue = System.getProperty(propertyName);
      if (propValue != null) {
        return propValue;
      }
    }

    return defaultValue;
  }

  public ElasticSearchInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    if(connector == null) {
      try {
        connector = new ESConnector(this.getProperty());
      }catch(Exception e){
        this.connector = null;
      }
    }

    if(sc == null){
      sc = new HiveContext(
          new SparkContext(
              new SparkConf()
                  .setMaster("local[*]")
                  .setAppName("elasticSearchInterpret")
                  .set("es.nodes", "cep1")
          )
      );
    }
  }

  @Override
  public void close() {
    //접속 끊는거 정리해 두기..
    connector.getClient().close();
    this.sc.sparkContext().cancelAllJobs();
  }

  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext interpreterContext) {
    try{
      IFetcher fetcher = null;
      FetcherContext context = new FetcherContext(this.getProperty(), sc, connector, null, null);
      FetcherFactory factory = new FetcherFactory(cmd);

      while(factory.hasNext()){
        fetcher = factory.next(context);
        fetcher.fetch();
        context = fetcher.getContext();
      }

      String resultData = null;

      if (fetcher != null) {
        resultData = fetcher.printTable();
      }

      return new InterpreterResult(InterpreterResult.Code.SUCCESS, resultData);
    }catch(Exception e){
      e.printStackTrace();
      return new InterpreterResult(InterpreterResult.Code.ERROR, e.getMessage());
    }
  }

  @Override
  public void cancel(InterpreterContext interpreterContext) {
  }

  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext interpreterContext) {
    return 0;
  }

  @Override
  public List<String> completion(String s, int i) {
    return null;
  }

  public static void main(String[] args) throws Exception{
    Properties props = new Properties();
    props.setProperty("server.list", "cep1:9300");
    props.setProperty("cluster.name", "cep");
    props.setProperty("zeppelin.elasticsearch.maxResult", "10000");

    ElasticSearchInterpreter interpreter = new ElasticSearchInterpreter(props);
    interpreter.open();
    //InterpreterResult result = interpreter.interpret("SELECT COUNT(*) as userCnt, MIN(balance) as minBalance, MAX(balance) as maxBalance FROM elasticsearch-sql_test_index/account GROUP BY age ORDER BY age|linear method=regression, label=age, features={minBalance}, optimization=SGD, mode=train, numberOfIterations=15|timechart(key=age, value=minBalance)", null);
//    InterpreterResult result = interpreter.interpret("load file:///Users/i1befree/Downloads/loc_std1_201504301453.dat|Fields Field13, Field8, Field12, Field14, Field16|rename Field13, Field14 to Lang, Long|add test with concat(Field8, Field12)|mapchart(latitude=Lang,longitude=Long,value=Field16)", null);
//    InterpreterResult result = interpreter.interpret("SELECT COUNT(*) as userCnt, MIN(balance) as minBalance, MAX(balance) as maxBalance FROM elasticsearch-sql_test_index/account GROUP BY age,gender ORDER BY age, gender|rename gender to sex|multichart(key=age, value={bar:maxBalance, time:[minBalance, maxBalance]})", null);
    InterpreterResult result = interpreter.interpret("desc elasticsearch-sql_test_index/account", null);
//    InterpreterResult result = interpreter.interpret("SELECT COUNT(*) as userCnt, MIN(balance) as minBalance, MAX(balance) as maxBalance FROM elasticsearch-sql_test_index/account GROUP BY age,gender ORDER BY age, gender|rename gender to sex|bubblechart(x=age,y=minBalance,z=maxBalance, group=sex, size=userCnt)", null);
//    InterpreterResult result = interpreter.interpret("SELECT COUNT(*) as userCnt, MIN(balance) as minBalance, MAX(balance) as maxBalance FROM elasticsearch-sql_test_index/account GROUP BY age,gender ORDER BY age, gender|rename gender to sex|filter sex='f'|bubblechart(x=age,y=minBalance,z=maxBalance, group=sex, size=userCnt)", null);
//    InterpreterResult result = interpreter.interpret("source elasticsearch-sql_test_index/account|join account_number [source elasticsearch-sql_test_index/temp_account]", null);
//    InterpreterResult result = interpreter.interpret("load file:///Users/i1befree/workspace/esinterpreter/src/main/resources/sample/data1.txt|append [load file:///Users/i1befree/workspace/esinterpreter/src/main/resources/sample/data2.txt]", null);
//    InterpreterResult result = interpreter.interpret("source elasticsearch-sql_test_index/account|stats count(state) as state_cnt, avg(balance) as minBalance_avg by city", null);
//    InterpreterResult result = interpreter.interpret("SELECT COUNT(*) as userCnt, MIN(balance) as minBalance, MAX(balance) as maxBalance FROM elasticsearch-sql_test_index/account GROUP BY gender, age ORDER BY gender, age|timechart(key=age, group=gender, value=minBalance)", null);
//    InterpreterResult result = interpreter.interpret("SELECT COUNT(*) as userCnt, MIN(balance) as minBalance, MAX(balance) as maxBalance FROM elasticsearch-sql_test_index/account GROUP BY gender, age ORDER BY gender, age|correlation age, userCnt", null);
    System.out.println("[" + result.message() + "]");
  }
}
