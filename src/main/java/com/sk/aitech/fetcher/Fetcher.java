package com.sk.aitech.fetcher;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by i1befree on 15. 7. 3..
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Fetcher {
  String name();
  String[] alias() default {};
  String[] previousFetcher() default {};
  String description() default "";
}
