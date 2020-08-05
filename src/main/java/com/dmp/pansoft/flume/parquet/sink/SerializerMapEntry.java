package com.dmp.pansoft.flume.parquet.sink;

import com.dmp.pansoft.flume.parquet.serializer.ParquetSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Date;

/**
 * 类描述:
 *
 * @author: pxq
 * @date: 2020-08-03 09:20
 */
public class SerializerMapEntry {
    private Path workingPath;
    private Configuration configuration;
    private String targetPath;
    private ParquetSerializer serializer;
    private long startTime;

    public SerializerMapEntry(Path workingPath, Configuration configuration, String targetPath, ParquetSerializer serializer) {
        this.workingPath = workingPath;
        this.configuration = configuration;
        this.targetPath = targetPath;
        this.serializer = serializer;
        this.startTime = new Date().getTime();
    }

    public ParquetSerializer getSerializer() {
        return serializer;
    }

    public long getStartTime() {
        return startTime;
    }

    public void close() throws IOException {
        serializer.close();
        FileSystem fileSystem = workingPath.getFileSystem(configuration);
        Path dstPath = new Path(targetPath);
        fileSystem.rename(workingPath, dstPath);
    }
}
