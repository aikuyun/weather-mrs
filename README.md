# 天气分析实例

> 对历史天气数据分析的一个项目，先是使用爬虫程序从天气网站上获取北京是各省、市、区县近几年的天气数据，然后对数据进行实时分发、收集和统 计分析，最后通过 BI 工具进行图表展示。

## 原始数据
![](https://github.com/aikuyun/weather-mrs/blob/master/src/main/resources/%E5%8E%9F%E5%A7%8B%E6%95%B0%E6%8D%AE.png)

## 数据结果
![](https://github.com/aikuyun/weather-mrs/blob/master/src/main/resources/%E6%95%B0%E6%8D%AE%E7%BB%93%E6%9E%9C%E5%B1%95%E7%A4%BA.png)

## 解决方案
1.先建立一张 Hbase 表。

2.使用爬虫程序获取天气数据。

3.使用 kafak 实时分发数据。

4.利用 Flume 收集数据并写入到 Hbase 中。

5.创建 Hive 表与 Hbase 表进行关联。

6.使用 Superset 统计和展示 Hive 中的表。

## 数据流
![](https://github.com/aikuyun/weather-mrs/blob/master/src/main/resources/%E6%95%B0%E6%8D%AE%E6%B5%81%E5%90%91.png)

## 代码导航

- Hive
- HBase
- Kafka
- Crawl
- Flume