package com.dmp.pansoft.flume.parquet.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 类描述:
 *
 * @author: pxq
 * @date: 2020-08-03 09:19
 */
public class SerializerLinkedHashMap extends LinkedHashMap<String, SerializerMapEntry> {
    private static final Logger LOG = LoggerFactory.getLogger(SerializerLinkedHashMap.class);

    private final int maxOpenFiles;

    public SerializerLinkedHashMap(int maxOpenFiles) {
        super(maxOpenFiles, 0.75f, true); // stock initial capacity/load, access ordering
        this.maxOpenFiles = maxOpenFiles;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<String, SerializerMapEntry> eldest) {
        if (size() > maxOpenFiles) {
            try {
                eldest.getValue().close();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
            return true;
        } else {
            return false;
        }
    }

}
