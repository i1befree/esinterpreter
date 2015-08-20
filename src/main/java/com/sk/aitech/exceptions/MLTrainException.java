package com.sk.aitech.exceptions;

/**
 * ML Train 예외 처리용 클래스
 */
public class MLTrainException extends Exception {
  String message;

  public MLTrainException(String message){
    this.message = message;
  }

  public MLTrainException(Exception e){
    this.message = "IFetcher Exception : " + e.getMessage();
  }

  public String getMessage(){
    return this.message;
  }
}
