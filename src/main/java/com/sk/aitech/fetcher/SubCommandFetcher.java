package com.sk.aitech.fetcher;

import com.sk.aitech.exceptions.FetcherException;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by i1befree on 15. 7. 29..
 */
public abstract class SubCommandFetcher extends DefaultFetcher {
  @Override
  public abstract void fetch() throws FetcherException;

  public SubCommandFetcher(FetcherContext context) throws FetcherException{
    super(context);
  }

  protected FetcherContext fetchSubCmd(String subCmd, FetcherContext context) throws FetcherException {
    FetcherFactory factory = new FetcherFactory(subCmd);
    IFetcher fetcher;

    while (factory.hasNext()) {
      fetcher = factory.next(context);
      fetcher.fetch();
      context = fetcher.getContext();
    }

    return context;
  }

  protected StructField[] mergeField(StructField[] fields1, StructField[] fields2){
    List<StructField> allFields = new ArrayList<>();
    Collections.addAll(allFields, fields1);

    for(StructField field2 : fields2){
      boolean findCol = false;

      for(StructField field : allFields){
        if(field2.equals(field)){
          findCol = true;
          break;
        }
      }

      if(!findCol)
        allFields.add(field2);
    }

    return allFields.toArray(new StructField[0]);
  }
}
