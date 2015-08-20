package com.sk.aitech.interpreter;

import org.elasticsearch.search.aggregations.bucket.terms.Terms;

/**
 * ESSQL 에서 Aggregation 데이터 처리를 위한 기본 처리 단위.
 */
public class BucketTuple {
  private String bucketName;
  private Terms.Bucket bucket;

  public BucketTuple(String bucketName, Terms.Bucket bucket){
    this.bucketName = bucketName;
    this.bucket = bucket;
  }

  public String getBucketName() {
    return bucketName;
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public Terms.Bucket getBucket() {
    return bucket;
  }

  public void setBucket(Terms.Bucket bucket) {
    this.bucket = bucket;
  }
}
