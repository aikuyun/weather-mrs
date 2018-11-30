package com.cuteximi.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: weathermrs
 * @description: 操作 Hbase
 * @author: TSL
 * @create: 2018-11-30 22:28
 **/
public class HbaseOpration {
    private static final Log LOG = LogFactory.getLog(HBaseOperation.class.getName());
    private Configuration conf = null;
    private static Connection conn = null;

    public HBaseOperation(Configuration conf) throws IOException {
        this.conf = conf;
        conn = ConnectionFactory.createConnection(conf);
    }

    public void createTable(TableName tableName) {
        HTableDescriptor htd = new HTableDescriptor(tableName);
        HColumnDescriptor hcd = new HColumnDescriptor("info");
        hcd.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
        hcd.setCompressionType(Algorithm.SNAPPY);
        htd.addFamily(hcd);
        Admin admin = null;

        try {
            admin = conn.getAdmin();
            if (!admin.tableExists(tableName)) {
                LOG.info("Creating table...");
                admin.createTable(htd);
                LOG.info(admin.getClusterStatus());
                LOG.info(admin.listNamespaceDescriptors());
                LOG.info("Table created successfully.");
            } else {
                LOG.warn("table already exists");
            }
        } catch (IOException var14) {
            LOG.error("Create table failed.", var14);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException var13) {
                    LOG.error("Failed to close admin ", var13);
                }
            }

        }

    }

    public static void putData(String line, TableName tableName) {
        Table table = null;

        try {
            table = conn.getTable(tableName);
            List<Put> puts = new ArrayList();
            String[] fields = line.split(",", -1);
            String rowkey = fields[0] + " " + fields[4];
            Put put = new Put(Bytes.toBytes(rowkey));
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
            puts.add(put);
            table.put(puts);
        } catch (IOException var15) {
            LOG.error("Put failed ", var15);
        } finally {
            if (table != null) {
                try {
                    table.close();
                } catch (IOException var14) {
                    LOG.error("Close table failed ", var14);
                }
            }

        }

    }

    public void dropTable(TableName tableName) {
        Admin admin = null;

        try {
            admin = conn.getAdmin();
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            LOG.info("Drop table successfully.");
        } catch (IOException var12) {
            LOG.error("Drop table failed ", var12);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException var11) {
                    LOG.error("Close admin failed ", var11);
                }
            }

        }

    }

    public void clean() {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception var2) {
                LOG.error("Failed to close the connection ", var2);
            }
        }

    }
}
