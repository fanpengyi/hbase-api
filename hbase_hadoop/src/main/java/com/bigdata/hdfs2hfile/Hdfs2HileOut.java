package com.bigdata.hdfs2hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * 将HDFS文件写成Hfile格式输出
 */
public class Hdfs2HileOut extends Configured implements Tool {

    public static void main(String[] args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop104:2181");

        int run = ToolRunner.run(configuration, new Hdfs2HileOut(), args);

        System.exit(run);


    }



    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = super.getConf();

        Job job = Job.getInstance(conf);
        job.setJarByClass(Hdfs2HileOut.class);


        FileInputFormat.addInputPath(job,new Path("hdfs://hadoop102:9000/hbase/input"));

        job.setMapperClass(Hdfs2HFileMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("user2"));

        //使MR可以向user2表中，增量增加数据
        HFileOutputFormat2.configureIncrementalLoad(job,table,connection.getRegionLocator(TableName.valueOf("user2")));
        //数据写回到HDFS 写成HFILE -》 所以指定输出格式为Hfile
        job.setOutputFormatClass(HFileOutputFormat2.class);

        HFileOutputFormat2.setOutputPath(job,new Path("hdfs://hadoop102:9000/hbase/out_hfile2"));

        //开始执行
        boolean b = job.waitForCompletion(true);

        return b? 0: 1;
    }
}
