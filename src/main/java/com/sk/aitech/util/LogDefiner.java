package com.sk.aitech.util;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by i1befree on 15. 7. 22..
 */
public class LogDefiner implements Serializable{
  private static final Pattern p = Pattern.compile("[^\\w|\\.]+");

  private String seperateStr = null;
  private List<String> logs;
  private List<StructField> fieldStructs;

  public LogDefiner(List<String> logs) {
    this.logs = logs;
    this.fieldStructs = new ArrayList<>();

    findSeperator();
  }

  /**
   * 로그의 자료 구조를 판단한다.
   *
   * @return
   */
  public StructType getSchema() {
    if (logs.size() == 0 || this.seperateStr == null) {
      fieldStructs.add(DataTypes.createStructField("Field", DataTypes.StringType, true));
    } else {
      String line = logs.get(0);
      String[] fields = line.split(this.seperateStr);

      for (int i = 1; i <= fields.length; i++) {
        fieldStructs.add(DataTypes.createStructField("Field" + i, DataTypes.StringType, true));
      }
    }

    return DataTypes.createStructType(fieldStructs);
  }

  public void findSeperator() {
    if (logs.size() > 0) {
      int max = 0;

      Map<String, Integer> seperateStrMap = new TreeMap<>();

      for (String line : logs) {
        //가장 많이 나온 구분자를 찾아서 그걸로 1차 자동 구분 시도
        Matcher m = p.matcher(line);
        int start = 0;

        while (m.find(start)) {
          String seperator = line.substring(m.start(), m.end());
          start = m.end() + 1;
          seperateStrMap.put(seperator, seperateStrMap.containsKey(seperator)? seperateStrMap.get(seperator) + 1 : 1);
        }
      }

      String seperateKey = null;

      for (int i = 0; i < seperateStrMap.keySet().size(); i++) {
        for (String key : seperateStrMap.keySet()) {
          if (max < seperateStrMap.get(key)) {
            max = seperateStrMap.get(key);
            //제안할 구분자
            seperateKey = key;
          }
        }

        //제안된 구분자로 각 라인별 필드 카운트가 일치하는 지 검증하기
        if (seperateKey == null)
          break;

        if (isValidSeperator(seperateKey)) {
          this.seperateStr = seperateKey;
          break;
        }
      }
    }
  }

  private boolean isValidSeperator(String seperateKey) {
    int fieldCnt = 0;

    for (int lineNumber = 0; lineNumber < logs.size(); lineNumber++) {
      if (lineNumber == 0)
        fieldCnt = logs.get(lineNumber).split(seperateKey).length;
      else {
        if (fieldCnt != logs.get(lineNumber).split(seperateKey).length)
          return false;
      }
    }

    return true;
  }

  public String getSeperateStr() {
    return seperateStr;
  }
}
