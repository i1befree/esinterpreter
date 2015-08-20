package com.sk.aitech.fetcher;

import com.sk.aitech.exceptions.FetcherException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.nlpcn.es4sql.exception.SqlParseException;

import java.io.Serializable;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;

/**
 * Created by i1befree on 15. 6. 29..
 */
public interface IFetcher extends Serializable{
  public void fetch() throws FetcherException;
  public List<String> getRows();
  public List<String> getFields();
  public String printTable();
  public FetcherContext getContext();
}
