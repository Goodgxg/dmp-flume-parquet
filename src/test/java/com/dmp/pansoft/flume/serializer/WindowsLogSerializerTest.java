package com.dmp.pansoft.flume.serializer;


import com.dmp.pansoft.flume.commons.AbstractReflectionAvroEventSerializer;
import com.dmp.pansoft.flume.event.WindowsLogEvent;
import com.dmp.pansoft.flume.parquet.serializer.WindowsLogSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.IOException;

import static com.dmp.pansoft.flume.serializer.JsonTestData.*;


public class WindowsLogSerializerTest {
    private WindowsLogSerializer sut;
    private Schema schema;

    @Before
    public void startUp() throws IOException {
        sut = new WindowsLogSerializer();
        Context context = new Context();
        sut.configure(context);
        schema = new Schema.Parser().parse(AbstractReflectionAvroEventSerializer.createSchema(WindowsLogEvent.class));
        Path fileToWrite = new Path("/parquet/data" + System.currentTimeMillis() + ".parquet");
        ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();
        sut.initialize(writer);
    }

    @After

    public void tearDown() throws IOException {
        sut.close();
    }

    @Test
    public void testEventCreation() throws Exception {
        testEventCreation(TEST_INPUT_1.getBytes());
        testEventCreation(TEST_INPUT_2.getBytes());
        testEventCreation(TEST_INPUT_3.getBytes());
        testEventCreation(TEST_INPUT_4.getBytes());
        testEventCreation(TEST_INPUT_5.getBytes());
        testEventCreation(TEST_INPUT_6.getBytes());
        testEventCreation(TEST_INPUT_7.getBytes());
        testEventCreation(TEST_INPUT_8.getBytes());
        testEventCreation(TEST_INPUT_9.getBytes());
    }

    @Test
    public void createSchema() throws Exception {
        System.out.println(schema.toString());
    }

    public void testEventCreation(byte[] testDaten) throws Exception {
        Event event = new SimpleEvent();
        event.setBody(testDaten);
        sut.write(event);
    }
}