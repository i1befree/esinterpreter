package com.sk.aitech.fetcher;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by i1befree on 15. 7. 7..
 */
public class RegTest {
  static enum ELEMENT {
    COLUMNNAME,
    EQUATION
  }

  public static void main(String[] args) throws Exception{
//    String evalString = "timechart(key=seriesX, groups=seriesY, value=field3)";
    System.out.println(ELEMENT.EQUATION.name());
    System.out.println(Double.parseDouble("2726.0"));
    String cmd = "source elasticsearch-sql_test_index/account|join account_number [source elasticsearch-sql_test_index/temp_account]";
//    Pattern p1 = Pattern.compile("rename\\s*\\{?(?<oldname>(\\w+\\s*,?\\s*)+)\\}?\\s*to\\s*\\{?(?<newname>(\\w+\\s*,?\\s*)+)\\}?\\s*");
//    Pattern p2 = Pattern.compile("((\\s*\\w+\\s*=\\s*\\w+)|(\\s*\\w+\\s*=\\s*\\{(\\s*\\w+\\s*,?\\s*)*}*))");

    int start = 0;
    int subCmdCnt = 0;
    Map<String, String> subCmdMap = new HashMap<>();
    Pattern subCmdP = Pattern.compile("\\[.*\\]\\s*\\||\\[.*\\]\\s*$");

    Matcher m = subCmdP.matcher(cmd);

    while(m.find(start)){
      subCmdCnt++;

      String subCmd = cmd.substring(m.start(), m.end());

      if(subCmd.endsWith("|"))
        subCmd = subCmd.substring(0, subCmd.length() - 1);

      String subCmdKey = "$subcmd$" + subCmdCnt;
      subCmdMap.put(subCmdKey, subCmd);
      start = m.end();
    }

    for(String key : subCmdMap.keySet()) {
      cmd = cmd.replace(subCmdMap.get(key), key);
    }

    String[] cmdLines = cmd.split("\\|");

    for(String cmdLine : cmdLines){
      for(String key : subCmdMap.keySet()) {
        cmdLine = cmdLine.replace(key, subCmdMap.get(key));
      }

      System.out.println(cmdLine);
    }
  }
}
