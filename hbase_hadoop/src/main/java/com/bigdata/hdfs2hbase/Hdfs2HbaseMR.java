package com.bigdata.hdfs2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class Hdfs2HbaseMR {

    public static class HdfsMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

        /**
         * HDFS -- Hbase
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //数据原样输出
            context.write(value,NullWritable.get());
        }
    }



    public static class HBASEReducer extends TableReducer<Text,NullWritable,ImmutableBytesWritable>{


        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

            /**
             * key --> 一行数据
             * 样例数据：
             * 0007 zhangsan 18
             * 0008 lisi 25
             * 0009 wangwu 20
             *
             */
            //按格式拆分
            String[] split = key.toString().split("\t");
            //构建 put 对象
            Put put = new Put(Bytes.toBytes(split[0]));
            put.addColumn("f1".getBytes(),"name".getBytes(),split[1].getBytes());
            put.addColumn("f1".getBytes(),"age".getBytes(),split[2].getBytes());

            context.write(new ImmutableBytesWritable(split[0].getBytes()),put);

        }
    }


        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

            Configuration configuration = HBaseConfiguration.create();


            configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop104:2181");

            Job job = Job.getInstance(configuration);

            job.setJarByClass(Hdfs2HbaseMR.class);

            //输入文件路径

            FileInputFormat.addInputPath(job,new Path("hdfs://hadoop102:9000/hbase/input"));

            job.setMapperClass(HdfsMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            //指定输出到 Hbase 的 表名
            TableMapReduceUtil.initTableReducerJob("user2",HBASEReducer.class,job);

            //设置 reduce 个数
            job.setNumReduceTasks(1);

            boolean b = job.waitForCompletion(true);

            System.exit(b?0:1);


        }




}


