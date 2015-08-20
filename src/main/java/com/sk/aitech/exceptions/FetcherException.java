package com.sk.aitech.exceptions;

/**
 * Created by i1befree on 15. 7. 1..
 */
public class FetcherException extends Exception {
  String message;

  public FetcherException(String message){
    this.message = message;
  }

  public FetcherException(Exception e){
    this.message = "IFetcher Exception : " + e.getMessage();
  }

  public String getMessage(){
    return this.message;
  }
}
