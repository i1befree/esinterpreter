package com.sk.aitech.exceptions;

/**
 * Created by i1befree on 15. 8. 12..
 */
public class MLPredictionException extends Exception {
  String message;

  public MLPredictionException(String message){
    this.message = message;
  }

  public MLPredictionException(Exception e){
    this.message = "MLPredictionException : " + e.getMessage();
  }

  public String getMessage(){
    return this.message;
  }
}
