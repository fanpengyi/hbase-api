package com.bigdata.hbase2hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * myuser f1 :name&age => myuser f1
 *
 * HbaseMapper  extends TableMapper <Text,Put>
 *
 *     key -- rowKey
 *     result -- data
 *
 */
public class HBaseReadMapper extends TableMapper<Text,Put> {

        /**
         *
         * @param key rowkey
         * @param value rowkey 此行的数据  Result 类型
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            //获得rowkey 的字节数组
            byte[] rowkey_bytes = key.get();
            String rowKeyStr = Bytes.toString(rowkey_bytes);
            //准备好 put 对象 用于输出下游
            Put put = new Put(rowkey_bytes);
            //text 作为输出的 key
            Text text = new Text(rowKeyStr);
            //输出数据 - 写数据 - 普通 构建put 对象
            Cell[] cells = value.rawCells();
            //将 f1 : name & age 输出
            for (Cell cell : cells) {
                //当前 cell是否是 f1
                //获取列族
                byte[] family = CellUtil.cloneFamily(cell);
                String familyStr = Bytes.toString(family);

                if("f1".equals(familyStr)){
                    //在判断是否是 name | age
                    put.add(cell);
                }

                if("f2".equals(familyStr)){
                    put.add(cell);
                }
            }

            if(!put.isEmpty()){
                context.write(text,put);
            }

        }
    }


