package com.bigdata.hbase2hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * hbaseReducer extends TableReducer
 *
 * ImmutableBytesWritable -- 设置 rowkey
 */
public class HbaseWriteReducer extends TableReducer<Text,Put,ImmutableBytesWritable> {

    /**
     * 将 map 传过来的数据写出去
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        //设置rowkey

        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
        //设置rowkey
        immutableBytesWritable.set(key.toString().getBytes());
        for (Put value : values) {
            context.write(immutableBytesWritable,value);
        }
    }
}
