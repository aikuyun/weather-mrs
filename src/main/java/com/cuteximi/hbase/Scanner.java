package com.cuteximi.hbase;

import java.io.File;
import java.io.IOException;

/**
 * @program: weathermrs
 * @description: 查询数据
 * @author: TSL
 * @create: 2018-11-30 22:29
 **/
public class Scanner {
    private static final Log LOG = LogFactory.getLog(Scanner.class.getName());
    private static String CONF_DIR;
    private static Connection conn;
    private static Configuration conf;
    private static TableName tableName;
    public String TABLE_NAME = "hbase_weather";

    public Scanner(Configuration conf) throws IOException {
        conf = conf;
        tableName = TableName.valueOf(this.TABLE_NAME);
        conn = ConnectionFactory.createConnection(conf);
    }

    public void scanData(String province, String city, String zone, String time) {
        Table table = null;
        ResultScanner rScanner = null;

        try {
            table = conn.getTable(tableName);
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("province"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("zone"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("time"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("maxTemperature"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("minTemperature"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("weather"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("windDirection"));
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("windPower"));
            FilterList list = new FilterList(Operator.MUST_PASS_ALL);
            list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("province"), CompareOp.EQUAL, Bytes.toBytes(province)));
            list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("city"), CompareOp.EQUAL, Bytes.toBytes(city)));
            list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("zone"), CompareOp.EQUAL, Bytes.toBytes(zone)));
            list.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"), Bytes.toBytes("time"), CompareOp.EQUAL, Bytes.toBytes(time)));
            scan.setFilter(list);
            rScanner = table.getScanner(scan);

            for(Result r = rScanner.next(); r != null; r = rScanner.next()) {
                Cell[] var10 = r.rawCells();
                int var11 = var10.length;

                for(int var12 = 0; var12 < var11; ++var12) {
                    Cell cell = var10[var12];
                    System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + ":" + Bytes.toString(CellUtil.cloneFamily(cell)) + "," + Bytes.toString(CellUtil.cloneQualifier(cell)) + "," + Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

            LOG.info("Scan data successfully.");
        } catch (IOException var22) {
            LOG.error("Scan data failed ", var22);
        } finally {
            if (rScanner != null) {
                rScanner.close();
            }

            if (table != null) {
                try {
                    table.close();
                } catch (IOException var21) {
                    LOG.error("Close table failed ", var21);
                }
            }

        }

    }

    public static void main(String[] args) throws IOException {
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: WeatherInfoCollector <in> <out>");
            System.exit(4);
        }

        String province = otherArgs[0];
        String city = otherArgs[1];
        String zone = otherArgs[2];
        String time = otherArgs[3];

        try {
            Scanner scanner = new Scanner(conf);
            scanner.scanData(province, city, zone, time);
        } catch (Exception var8) {
            LOG.error("Failed to scan HBase because ", var8);
        }

    }

    static {
        CONF_DIR = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        conn = null;
        conf = null;
        tableName = null;
    }
}

