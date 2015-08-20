package com.sk.aitech.exceptions;

/**
 * Created by i1befree on 15. 8. 12..
 */
public class MLLoadException extends Exception {
  String message;

  public MLLoadException(String message){
    this.message = message;
  }

  public MLLoadException(Exception e){
    this.message = "MLPredictionException : " + e.getMessage();
  }

  public String getMessage(){
    return this.message;
  }
}
