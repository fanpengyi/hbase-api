package com.bigdata.hdfs2hfile;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * 将HDFS 上超大文件写成 HFILE 格式，在于HBASE 关联起来
 */

public class Hdfs2HFileMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\t");

        //封装输出类型
        Put put = new Put(split[0].getBytes());
        put.addColumn("f1".getBytes(),"name".getBytes(),split[1].getBytes());
        put.addColumn("f1".getBytes(),"age".getBytes(),split[2].getBytes());
        // 将封装好的put对象输出，rowkey 使用 immutableBytesWritable

        context.write(new ImmutableBytesWritable(Bytes.toBytes(split[0])),put);

    }
}
