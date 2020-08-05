package com.dmp.pansoft.flume.converter;

import org.apache.avro.Schema;

/**
 * 类描述:
 *
 * @author: pxq
 * @date: 2020-07-31 15:40
 */
public class StringConverter implements Converter<String> {
    @Override
    public String converToTypeObject(Object untyped) throws Exception {
        return String.valueOf(untyped);
    }

    @Override
    public Schema.Type converToType(Object typed) {
        return typed instanceof String ? Schema.Type.STRING : null;
    }
}
