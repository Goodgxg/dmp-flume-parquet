package com.dmp.pansoft.flume.parquet.serializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;

import org.apache.parquet.hadoop.ParquetWriter;


import java.io.IOException;

/**
 * 类描述:
 *
 * @author: pxq
 * @date: 2020-08-03 09:06
 */
public interface ParquetSerializer extends EventSerializer, Configurable {
    void initialize(ParquetWriter<GenericData.Record> writer) throws Exception;

    ParquetWriter<GenericData.Record> getWriter();

    void close() throws IOException;

    Schema getSchema();
}
