package com.sk.aitech.fetcher;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by i1befree on 15. 7. 30..
 */
public class AppendTest {
  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void test() {
    //TODO: 경로 처리를 해야 함.
    /*
    String query = "load file:///Users/i1befree/workspace/esinterpreter/src/main/resources/sample/data1.txt|append [load file:///Users/i1befree/workspace/esinterpreter/src/main/resources/sample/data2.txt]";
    Properties props = new Properties();
    props.setProperty("server.list", "cep1:9300");
    props.setProperty("cluster.name", "cep");
    props.setProperty("zeppelin.elasticsearch.maxResult", "10000");

    ElasticSearchInterpreter interpreter = new ElasticSearchInterpreter(props);
    interpreter.open();

    InterpreterResult result = interpreter.interpret(query, null);
    assertEquals(result.type(), InterpreterResult.Type.TABLE);
    assertEquals(result.message(), "Field1\tField2\tField3\tField4\nvalue1\tvalue2\tvalue3\tvalue4\nvalue11\tvalue12\tvalue13\tnull\n");
    */
  }
}
