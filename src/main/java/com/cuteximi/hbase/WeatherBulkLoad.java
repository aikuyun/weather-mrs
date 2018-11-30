package com.cuteximi.hbase;


import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
/**
 * @program: weathermrs
 * @description: 导入数据到 hbase
 * @author: TSL
 * @create: 2018-11-30 22:31
 **/
public class WeatherBulkLoad {
    private static final Log LOG = LogFactory.getLog(WeatherBulkLoad.class);

    public WeatherBulkLoad() {
    }

    public static void loadIncrementalHFileToHBase(Configuration configuration, Path path, TableName tableName) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        LoadIncrementalHFiles loder = new LoadIncrementalHFiles(configuration);
        loder.doBulkLoad(path, new HTable(conf, tableName));
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        TableName tableName = TableName.valueOf("hbase_weather");
        HBaseOperation hbase = new HBaseOperation(conf);
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: WeatherInfoCollector <in> <out>");
            System.exit(2);
        }

        Path srcPath = new Path(otherArgs[0]);
        Path descPath = new Path(otherArgs[1]);
        hbase.createTable(tableName);
        Job job = new Job(conf, "Collect Weather Info");
        job.setJarByClass(WeatherBulkLoad.class);
        job.setMapperClass(WeatherBulkLoad.CollectionMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        HTable table = new HTable(conf, tableName);
        HFileOutputFormat2.configureIncrementalLoad(job, table, table.getRegionLocator());
        FileInputFormat.addInputPath(job, srcPath);
        FileOutputFormat.setOutputPath(job, descPath);
        if (job.waitForCompletion(true)) {
            loadIncrementalHFileToHBase(conf, descPath, tableName);
        }

        hbase.clean();
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class CollectionMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
        public CollectionMapper() {
        }

        public void map(Object key, Text value, Mapper<Object, Text, ImmutableBytesWritable, Put>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",", -1);
            byte[] rowkey = Bytes.toBytes(fields[0] + " " + fields[4]);
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(rowkey);
            Put put = new Put(rowkey);
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(fields[0]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("province"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"), Bytes.toBytes(fields[2]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("zone"), Bytes.toBytes(fields[3]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"), Bytes.toBytes(fields[4]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("maxTemperature"), Bytes.toBytes(fields[5]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("minTemperature"), Bytes.toBytes(fields[6]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weather"), Bytes.toBytes(fields[7]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("windDirection"), Bytes.toBytes(fields[8]));
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("windPower"), Bytes.toBytes(fields[9]));
            context.write(rowKey, put);
        }
    }
}

