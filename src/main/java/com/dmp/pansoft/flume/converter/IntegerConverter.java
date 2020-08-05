package com.dmp.pansoft.flume.converter;

import org.apache.avro.Schema;

/**
 * 类描述:
 *
 * @author: pxq
 * @date: 2020-07-31 15:36
 */
public class IntegerConverter implements Converter<Integer> {
    @Override
    public Integer converToTypeObject(Object untyped) throws Exception {
        return Integer.parseInt(String.valueOf(untyped));
    }

    @Override
    public Schema.Type converToType(Object typed) {
        return typed instanceof Integer ? Schema.Type.INT : null;
    }
}
