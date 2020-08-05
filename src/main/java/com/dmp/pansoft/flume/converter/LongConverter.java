package com.dmp.pansoft.flume.converter;

import org.apache.avro.Schema;

/**
 * 类描述:
 *
 * @author: pxq
 * @date: 2020-07-31 15:38
 */
public class LongConverter implements Converter<Long> {
    @Override
    public Long converToTypeObject(Object untyped) throws Exception {
        return Long.parseLong(String.valueOf(untyped));
    }

    @Override
    public Schema.Type converToType(Object typed) {
        return typed instanceof Long? Schema.Type.LONG:null;
    }
}
