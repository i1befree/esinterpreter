package com.sk.aitech.fetcher;

import com.sk.aitech.exceptions.FetcherException;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 명령어 해석을 통해 Fetcher 를 생성하는 기능 수행.
 */
public class FetcherFactory {
  final static String FETCHER_PACKAGE_PATH = "com.sk.aitech.fetcher";
  final static String CMD_SEPERATOR = "\\|";

  final static Pattern p = Pattern.compile("\\w+");
  final static Pattern subCmdP = Pattern.compile("\\[.*\\]\\s*\\||\\[.*\\]\\s*$");
  private static Map<String, Class<?>> fetcherMap;

  static {
    fetcherMap = scanFetcher();
  }

  String cmd;
  List<String> cmdList;
  int currentCmdIndex;

  public FetcherFactory(String cmd){
    this.currentCmdIndex = 0;
    this.cmd = cmd;
    this.cmdList = new ArrayList<>();

    parseCommand(cmd);
  }

  private void parseCommand(String cmd) {
    int start = 0;
    Map<String, String> subCmdMap = new HashMap<>();
    int subCmdCnt = 0;
    Matcher m = subCmdP.matcher(cmd);

    while(m.find(start)){
      subCmdCnt++;

      String subCmd = cmd.substring(m.start(), m.end());
      String subCmdKey = "$subcmd$" + subCmdCnt;
      subCmdMap.put(subCmdKey, subCmd);
      start = m.end();
    }

    for(String key : subCmdMap.keySet()) {
      cmd = cmd.replace(subCmdMap.get(key), key);
    }

    String[] cmdLines = cmd.split(CMD_SEPERATOR);

    for(String cmdLine : cmdLines){
      for(String key : subCmdMap.keySet()) {
        cmdLine = cmdLine.replace(key, subCmdMap.get(key));
      }

      cmdList.add(cmdLine);
    }
  }

  public IFetcher next(FetcherContext context) throws FetcherException{
    return getFetcher(
        new FetcherContext(
            context.getEnvProperties(),
            context.getSparkContext(),
            context.getConnector(),
            context.getDf(),
            this.cmdList.get(this.currentCmdIndex++)
        )
    );
  }

  public boolean hasNext(){
    return this.cmdList.size() > this.currentCmdIndex;
  }

  public IFetcher getFetcher(FetcherContext context) throws FetcherException {
    Matcher m = p.matcher(context.getCmd());

    if (m.find()) {
      String firstWord = context.getCmd().substring(m.start(), m.end());

      try {
        return getFetcher(firstWord.toUpperCase(), context);
      }catch (InvocationTargetException e) {
        throw new FetcherException("Cannot invoke constructor for " + firstWord + ". The reason is " + e.getMessage());
      } catch (InstantiationException e) {
        throw new FetcherException("Cannot instantiate for " + firstWord + ". The reason is " + e.getMessage());
      } catch (IllegalAccessException e) {
        throw new FetcherException("Cannot access for " + firstWord + ". The reason is " + e.getMessage());
      }
    } else
      throw new FetcherException(String.format("Unsupported command: %s", context.getCmd()));
  }

  public static Map<String, Class<?>> scanFetcher(){
    Map<String, Class<?>> map = new HashMap<>();
    Reflections reflections = new Reflections(FETCHER_PACKAGE_PATH);
    Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(Fetcher.class);

    if(annotated.size() > 0){
      for(Class<?> item : annotated) {
        Annotation[] annotations = item.getAnnotations();

        for (Annotation annotation : annotations)
          if (annotation instanceof Fetcher) {
            map.put(((Fetcher) annotation).name().toUpperCase(), item);

            if(((Fetcher)annotation).alias() != null && ((Fetcher)annotation).alias().length > 0){
              for(String alias : ((Fetcher)annotation).alias()){
                map.put(alias.toUpperCase(), item);
              }
            }
          }
      }
    }

    return map;
  }

  public IFetcher getFetcher(String name, FetcherContext context) throws IllegalAccessException, InvocationTargetException, InstantiationException, FetcherException {
    Class fetcherClass = fetcherMap.get(name.toUpperCase());

    if(fetcherClass != null){
      Constructor[] constructors = fetcherClass.getDeclaredConstructors();

      for(Constructor constructor : constructors){
        Class[] paramTypes = constructor.getParameterTypes();

        if(paramTypes != null && paramTypes.length == 1 && paramTypes[0].equals(FetcherContext.class))
          return (IFetcher)constructor.newInstance(context);
      }
    }

    return null;
  }
}
