package com.sk.aitech.fetcher;

import com.sk.aitech.elasticsearch.ESConnector;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by i1befree on 15. 7. 24..
 */
public class FetcherContext {
  private ESConnector connector;
  private SQLContext sparkContext;
  private String cmd;
  private DataFrame df;
  private Properties envProperties;
  private Map<String, Object> config;

  public FetcherContext(Properties envProperties, SQLContext sc, ESConnector connector, DataFrame df, String cmd){
    this.envProperties = envProperties;
    this.sparkContext = sc;
    this.connector = connector;
    this.df = df;
    this.cmd = cmd;
    this.config = new HashMap<>();
  }

  public SQLContext getSparkContext() {
    return sparkContext;
  }

  public ESConnector getConnector(){
    return this.connector;
  }

  public String getCmd() {
    return cmd;
  }

  public DataFrame getDf() {
    return df;
  }

  public void setDf(DataFrame df){
    this.df = df;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public void addConfig(String key, Object value){
    this.config.put(key, value);
  }

  public Object getConfig(String configKey){
    return this.config.get(configKey);
  }

  public Properties getEnvProperties() {
    return envProperties;
  }

  public StructField[] getFields(){
    return this.df.schema().fields();
  }
}
