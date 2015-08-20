package com.sk.aitech.fetcher.ml;

import com.sk.aitech.exceptions.MLLoadException;
import com.sk.aitech.exceptions.MLPredictionException;
import com.sk.aitech.exceptions.MLSaveException;
import com.sk.aitech.exceptions.MLTrainException;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Trainer 인터페이스. Trainer는 특정 ML 을 훈련시키며, 훈련된 모델을 특정한 디렉터리에 보관한다.
 * 저장된 모델은 다시 불러들여 예측에 활용할 수 있다.
 */
public interface Trainer {
  public void train() throws MLTrainException;
  public DataFrame predict(DataFrame df, SQLContext sc) throws MLPredictionException;
  public void load(int modelId) throws MLLoadException;
  public int save() throws MLSaveException;
}
