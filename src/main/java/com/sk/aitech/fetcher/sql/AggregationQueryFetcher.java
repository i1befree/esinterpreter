package com.sk.aitech.fetcher.sql;

import com.sk.aitech.exceptions.FetcherException;
import com.sk.aitech.fetcher.FetcherContext;
import com.sk.aitech.fetcher.IFetcher;
import com.sk.aitech.interpreter.BucketTuple;
import com.sk.aitech.util.DataFrameUtil;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.aggregations.metrics.min.InternalMin;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.nlpcn.es4sql.exception.SqlParseException;
import org.nlpcn.es4sql.query.AggregationQueryAction;
import org.nlpcn.es4sql.query.QueryAction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by i1befree on 15. 6. 29..
 */
public class AggregationQueryFetcher implements IFetcher {
  List<String> rows;
  Map<String, String> columnMap;
  List<StructField> columnInfo;
  QueryAction queryAction;
  FetcherContext context;

  public AggregationQueryFetcher(FetcherContext context, org.nlpcn.es4sql.domain.Select select){
    this.context = context;
    this.queryAction = new AggregationQueryAction(context.getConnector().getClient(), select);;
    this.columnMap = new LinkedHashMap<>();
    this.columnInfo = new ArrayList<>();
  }

  @Override
  public List<String> getRows() {
    return this.rows;
  }

  @Override
  public List<String> getFields() {
    List fields = new ArrayList();
    fields.addAll(this.columnMap.keySet());

    return fields;
  }

  @Override
  public void fetch() throws FetcherException {
    int limitSize = Integer.parseInt(this.context.getEnvProperties().getProperty("zeppelin.elasticsearch.maxResult"));
    SearchRequestBuilder requestBuilder = null;

    try {
      requestBuilder = (SearchRequestBuilder)queryAction.explain();
      requestBuilder.setSize(limitSize);
      Aggregations aggregations = requestBuilder.get().getAggregations();
      this.rows = getAggregationRows(null, null, aggregations);

      this.context.setDf(DataFrameUtil.makeDataFrame(this.context.getSparkContext(), this.getFields(), this.getRows()));
    } catch (SqlParseException e) {
      throw new FetcherException(e);
    }
  }

  private List<String> getAggregationRows(String bucketName, String bucketKey, Aggregations buckets){
    List<String> rows = new ArrayList<>();


    List<BucketTuple> subBuckets = getSubBucket(buckets);

    if(subBuckets.size() > 0){
      for(BucketTuple subBucket : subBuckets){
        if(bucketName != null){
          this.columnMap.put(bucketName, bucketKey);
        }

        List<String> newRows = getAggregationRows(subBucket.getBucketName(), subBucket.getBucket().getKey(), subBucket.getBucket().getAggregations());
        rows.addAll(newRows);
      }

      return rows;
    }else{
      if(bucketName != null){
        this.columnMap.put(bucketName, bucketKey);
      }

      for(Aggregation field : buckets.asList()){
        if(field instanceof ValueCount){
          this.columnMap.put(field.getName(), String.valueOf(((ValueCount) field).getValue()));
        }else if(field instanceof InternalMax){
          this.columnMap.put(field.getName(), String.valueOf(((InternalMax) field).getValue()));
        }else if(field instanceof InternalMin){
          this.columnMap.put(field.getName(), String.valueOf(((InternalMin) field).getValue()));
        }
      }

      StringBuilder sb = new StringBuilder();
      String[] fieldValues = this.columnMap.values().toArray(new String[0]);

      for(int i = 0 ; i < fieldValues.length ; i++){
        if(i > 0)
          sb.append("\t");

        sb.append(fieldValues[i]);
      }

      rows.add(sb.toString());

      return rows;
    }
  }

  private List<BucketTuple> getSubBucket(Aggregations aggregations) {
    List<BucketTuple> subBuckets = new ArrayList<>();

    try{
      for(Aggregation aggregation : aggregations.asList()){
        List<Terms.Bucket> buckets = ((Terms)aggregation).getBuckets();

        if(buckets != null && buckets.size() > 0){
          for(Terms.Bucket bucket : buckets){
            BucketTuple bucketTuple = new BucketTuple(aggregation.getName(), bucket);
            subBuckets.add(bucketTuple);
          }
        }
      }
    }catch(ClassCastException e){
      //CastException 이 발생하면 마지막에 도달했음을 의미한다.
    }

    return subBuckets;
  }

  @Override
  public String printTable() {
    StringBuilder sb = new StringBuilder("%table ");

    List<String> results = this.getRows();

    for(int i = 0 ; i < this.getFields().size() ; i++){
      if(i > 0)
        sb.append("\t");

      sb.append(this.getFields().get(i));
    }

    sb.append("\n");

    for(String result : results)
      sb.append(result).append("\n");

    return sb.toString();
  }

  @Override
  public FetcherContext getContext() {
    this.context.setDf(DataFrameUtil.makeDataFrame(this.context.getSparkContext(), this.getFields(), this.getRows()));
    return this.context;
  }
}
