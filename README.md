# dmp-flume-parquet
自定义sink源实现flume消费source数据并保存为parquet格式到hdfs，在整合impala进行查询操作。
使用：
1.创建resources资源目录，加入自己集群的hdfs-site.xml,core-site.xml文件。
2.配置flume的配置文件
