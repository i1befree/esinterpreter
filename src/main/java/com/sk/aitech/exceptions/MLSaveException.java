package com.sk.aitech.exceptions;

/**
 * MLSaveException
 */
public class MLSaveException extends Exception {
  String message;

  public MLSaveException(String message){
    this.message = message;
  }

  public MLSaveException(Exception e){
    this.message = "MLPredictionException : " + e.getMessage();
  }

  public String getMessage(){
    return this.message;
  }
}
