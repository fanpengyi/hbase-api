package com.bigdata.hbase2hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.lang.model.SourceVersion;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

public class Hbase2HbaseMR extends Configured implements Tool {

   public static void main(String[] args) throws Exception {
       Configuration configuration = HBaseConfiguration.create();
       configuration.set("hbase.zookeeper.quorum","hadoop102:2181,hadoop103:2181,hadoop104:2181");
       int run = ToolRunner.run(configuration, new Hbase2HbaseMR(), args);
       System.exit(run);


   }


    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf());
        job.setJarByClass(Hbase2HbaseMR.class);
        //mapper
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("user"),new Scan(), HBaseReadMapper.class,Text.class,Put.class,job);
        //reducer
        TableMapReduceUtil.initTableReducerJob("user2",HbaseWriteReducer.class,job);
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }
}
