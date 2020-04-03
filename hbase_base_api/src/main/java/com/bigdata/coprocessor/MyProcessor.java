package com.bigdata.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
/**
 * 协处理器 类似拦截器
 *
 */
import java.io.IOException;
import java.util.List;

/**
 *
 */
public class MyProcessor extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {

       //获取连接
        Configuration configuration = HBaseConfiguration.create();

        configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181:hadoop104:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);
        //涉及多个版本得问题
        List<Cell> cells = put.get("info".getBytes(), "name".getBytes());

        Cell nameCell = cells.get(0);
        Put put1 = new Put(put.getRow());

        put1.add(nameCell);

        Table table = connection.getTable(TableName.valueOf("user2"));

        table.put(put1);
        table.close();
        connection.close();
    }
}




