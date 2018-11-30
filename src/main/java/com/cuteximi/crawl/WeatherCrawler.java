package com.cuteximi.crawl;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.Iterator;
import java.util.Set;

import com.cuteximi.hbase.HBaseOperation;
import com.cuteximi.hive.ClientInfo;
import com.cuteximi.hive.HiveJDBC;
import com.cuteximi.web.DomTool;
import com.cuteximi.kafka.Producer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.http.impl.client.DefaultHttpClient;
//import org.apache.kafka.clients.producer.Producer;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * @program: weathermrs
 * @description: 爬取天气，以及建表，数据分发等
 * @author: TSL
 * @create: 2018-11-30 22:27
 **/
public class WeatherCrawler{
    private static final Log LOG = LogFactory.getLog(WeatherCrawler.class.getName());
    private static final int PROVINCE_TYPE = 1;
    private static final int ZONE_TYPE = 2;
    private static final int WEATHER_TYPE = 3;
    public static final String TOPIC = "weather";
    private static DomTool domTool = new DomTool();
    private static TableName hbaseTable = null;
    private static HBaseOperation hbase = null;
    private static Producer producerThread = null;

    public WeatherCrawler() {
    }

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        String hbaseTableName = "hbase_weather";
        String hiveTableName = "hive_weather";
        hbaseTable = TableName.valueOf(hbaseTableName);

        try {
            hbase = new HBaseOperation(conf);
        } catch (IOException var5) {
            LOG.error("Failed to init HBase because ", var5);
        }

        hbase.createTable(hbaseTable);
        createHiveTable(hiveTableName, hbaseTableName);
        producerThread = new Producer("weather");
        weatherCrawl("北京", "http://www.tianqihoubao.com/lishi/index.htm");
        LOG.info("-----------finish to crawl weather info-------------------");
    }

    private static void weatherCrawl(String diffProvince, String url) {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        Document dom = domTool.getDom(url, httpclient);
        Set<String> cityLinks = domTool.getLinks(dom, ".citychk", "dt", 1);
        Iterator var5 = cityLinks.iterator();

        while(true) {
            label49:
            while(true) {
                String province;
                String citylink;
                do {
                    if (!var5.hasNext()) {
                        httpclient.close();
                        return;
                    }

                    String citylinks = (String)var5.next();
                    province = citylinks.split(";")[0];
                    citylink = citylinks.split(";")[1];
                } while(!diffProvince.equals(province));

                Document cityDom = domTool.getDom(citylink, httpclient, "utf-8");
                if (null == cityDom) {
                    LOG.info("Error city link = " + citylink);
                } else {
                    Set<String> zonelinks = domTool.getLinks(cityDom, ".citychk", "dl", 2);
                    Iterator var11 = zonelinks.iterator();

                    while(true) {
                        while(true) {
                            if (!var11.hasNext()) {
                                continue label49;
                            }

                            String zoneLink = (String)var11.next();
                            String zoneId = zoneLink.split(";")[0];
                            String city = zoneLink.split(";")[1];
                            String zone = zoneLink.split(";")[2];
                            String zonelink = zoneLink.split(";")[3];
                            Document zoneDom = domTool.getDom(zonelink, httpclient);
                            if (null == zoneDom) {
                                LOG.info("Error zone link = " + zonelink);
                            } else {
                                String regionInfo = zoneId + "," + province + "," + city + "," + zone + ",";
                                if (null != regionInfo) {
                                    Set<String> weatherLinks = domTool.getLinks(zoneDom, ".wdetail", "a", 3);
                                    Iterator var20 = weatherLinks.iterator();

                                    while(var20.hasNext()) {
                                        String weatherlink = (String)var20.next();
                                        Document weatherDom = domTool.getDom(weatherlink, httpclient);
                                        if (null == weatherDom) {
                                            LOG.info("Error weather link = " + weatherlink);
                                        } else {
                                            crawl2Kafka(weatherDom, regionInfo);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static void crawl2Kafka(Document dom, String regionInfo) {
        Elements es = domTool.getElements(dom, ".wdetail", "tr");
        if (es.isEmpty()) {
            LOG.info("Save weather file error, element is null.");
        } else {
            es.remove(0);
            Iterator var3 = es.iterator();

            while(var3.hasNext()) {
                Element ele = (Element)var3.next();
                Elements cells = ele.select("td");
                StringBuilder line = new StringBuilder();
                line.append(regionInfo);

                for(int index = 0; index < cells.size(); ++index) {
                    String high;
                    if (index == 0) {
                        high = ((Element)cells.get(index)).text().replace("年", "-");
                        high = high.replace("月", "-");
                        high = high.replace("日", "");
                        line.append(high);
                    }

                    if (index == 1 || index == 3) {
                        line.append(((Element)cells.get(index)).text());
                    }

                    if (index == 2) {
                        high = ((Element)cells.get(index)).text().split("/")[0];
                        String low = ((Element)cells.get(index)).text().split("/")[1];
                        if (high.length() == 0 || low.length() == 0) {
                            continue;
                        }

                        String high1 = high.substring(0, high.length() - 2);
                        String low1 = low.substring(0, low.length() - 1);
                        line.append(high1 + "," + low1.trim());
                    }

                    if (index != cells.size() - 1) {
                        line.append(",");
                    } else {
                        line.append("\n");
                    }
                }

                producerThread.run(line.toString());
            }

        }
    }

    private static void createHiveTable(String hiveTable, String hbaseTable) {
        String clientProperties = System.getProperty("user.dir") + File.separator + "conf" + File.separator + "hiveclient.properties";

        ClientInfo clientInfo;
        boolean isSecurityMode;
        try {
            clientInfo = new ClientInfo(clientProperties);
            isSecurityMode = "KERBEROS".equalsIgnoreCase(clientInfo.getAuth());
        } catch (IOException var9) {
            LOG.error("Failed to login because ", var9);
            return;
        }

        String sql = "CREATE EXTERNAL TABLE IF NOT EXISTS " + hiveTable + "(rowkey string, id string,province string, city string, zone string, time string,maxTemperature int, minTemperature int, weather string, windPower string) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key,info:id,info:province,info:city,info:zone,info:time,info:maxTemperature,info:minTemperature,info:weather,info:windPower\") TBLPROPERTIES(\"hbase.table.name\" = \"" + hbaseTable + "\")";
        HiveJDBC hiveJdbc = new HiveJDBC(clientInfo, isSecurityMode);

        try {
            Connection connection = hiveJdbc.getConnection();
            HiveJDBC.execDDL(connection, sql);
        } catch (Exception var8) {
            var8.printStackTrace();
        }

    }
}
