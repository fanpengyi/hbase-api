package com.bigdata.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.testng.collections.Lists;

import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * 动态加载  -- 手动配置表权限
 *
 * alter 'mytable', METHOD => 'table_att','coprocessor'=>'hdfs://hadoop102:9000/processor/processor.jar|com.bigdata.coprocessor.MyProcessor|1001|'
 * 静态加载 -- 开启全表权限 需重启集群
 *
 */
public class TestObserver {

    @Test
    public void testPut() throws IOException {

        //获取连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102:2181,hadoop103:2181,hadoop104:2181");
        //创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table proc1 = connection.getTable(TableName.valueOf("user1"));


        Put put = new Put("1110001112".getBytes());

        put.addColumn("info".getBytes(),"name".getBytes(),"hello".getBytes());
        put.addColumn("info".getBytes(),"gender".getBytes(),"male".getBytes());
        put.addColumn("info".getBytes(),"nationality".getBytes(),"test".getBytes());

        proc1.put(put);
        proc1.close();
        connection.close();

        System.out.println("success");



    }

}
