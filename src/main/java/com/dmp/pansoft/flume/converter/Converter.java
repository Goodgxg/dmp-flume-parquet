package com.dmp.pansoft.flume.converter;

import org.apache.avro.Schema;

/**
 * 类描述:
 *
 * @author: pxq
 * @date: 2020-07-31 15:35
 */
public interface Converter<T> {
    T converToTypeObject(Object untyped) throws Exception;
    Schema.Type converToType(Object typed);

}
