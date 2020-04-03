package com.bigdata.hdfs2hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

public class LoadHFile2Hbase {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop104:2181");
        //获取数据库连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf("user2"));
        //构建 LoadIncrementalHfiles 加载 Hfile文件
        LoadIncrementalHFiles loadIncrementalHFiles = new LoadIncrementalHFiles(configuration);
        // 加载上一步输出的HFile 与表做映射
        loadIncrementalHFiles.doBulkLoad(new Path("hdfs://hadoop102:9000/hbase/out_hfile2"),connection.getAdmin(),table,connection.getRegionLocator(TableName.valueOf("user2")));


    }

}
