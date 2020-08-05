package com.dmp.pansoft.flume.utils;


import com.dmp.pansoft.flume.converter.Converter;
import com.dmp.pansoft.flume.converter.IntegerConverter;
import com.dmp.pansoft.flume.converter.LongConverter;
import com.dmp.pansoft.flume.converter.StringConverter;
import org.apache.avro.Schema;


import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 类描述:
 *
 * @author: pxq
 * @date: 2020-07-31 15:30
 */
public class DynamicAvroSchemaCreator {
    private final Map<Class, Converter> converterMap;

    public DynamicAvroSchemaCreator() {
        converterMap = new LinkedHashMap<>();
        converterMap.put(Integer.class, new IntegerConverter());
        converterMap.put(Long.class, new LongConverter());
        converterMap.put(String.class, new StringConverter());
    }


    public Schema cretateSchema(Map<String, Object> data) throws Exception {
        List<Schema.Field> fields = cretateFields(data);
        Schema schema = Schema.createRecord("Event", null, null, false);
        Method setFields = schema.getClass().getDeclaredMethod("setFields", List.class);
        setFields.setAccessible(true);
        setFields.invoke(schema, fields);
        return schema;
    }

    private List<Schema.Field> cretateFields(Map<String, Object> data) throws Exception {
        List<Schema.Field> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            List<Schema> types = new ArrayList<>();
            types.add(Schema.create(Schema.Type.NULL));
            types.add(Schema.create(duceType(entry)));
            Schema typeSchema = Schema.createUnion(types);
            Schema.Field field = new Schema.Field(entry.getKey(), typeSchema, null, null, Schema.Field.Order.ASCENDING);
            fields.add(field);
        }

        return fields;
    }

    private Schema.Type duceType(Map.Entry<String, Object> entry) {
        Object typed = getTypedObject(entry.getValue());
        for (Converter converter : converterMap.values()) {
            Schema.Type type = converter.converToType(typed);
            if (type != null) {
                return type;
            }
        }
        throw new IllegalStateException("Unmappable type:" + typed);
    }

    private Object getTypedObject(Object value) {
        for (Converter converter : converterMap.values()) {
            try {
                Object typed = converter.converToTypeObject(value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        throw new IllegalStateException("Unmappable type: " + value);
    }

    public <T> T convertByType(Class<T> type, Object untyped) throws Exception {
        Converter<T> converter = converterMap.get(type);
        return converter.converToTypeObject(untyped);
    }

}
