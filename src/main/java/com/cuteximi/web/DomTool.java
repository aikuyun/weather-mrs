package com.cuteximi.web;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 * @program: weathermrs
 * @description: 网络工具
 * @author: TSL
 * @create: 2018-11-30 22:35
 **/
public class DomTool {
    private static final Log LOG = LogFactory.getLog(DomTool.class.getName());

    public DomTool() {
    }

    public Document getDom(String url, DefaultHttpClient httpClient, String charset) {
        HttpGet get = new HttpGet(url);
        HttpResponse response = null;
        Object dom = null;

        try {
            response = httpClient.execute(get);
            int statusCode = response.getStatusLine().getStatusCode();
            if (502 == statusCode) {
                for(int i = 1; statusCode != 200 && i < 4; ++i) {
                    response.getEntity().getContent().close();
                    Thread.currentThread();
                    Thread.sleep(10000L);
                    System.out.println("sleep " + i * 10 + "s");
                    response = httpClient.execute(get);
                    statusCode = response.getStatusLine().getStatusCode();
                }
            }

            if (statusCode != 200) {
                LOG.info("Status code is not 200, ignore...statusCode is " + statusCode + ". url = " + url);
            }

            String html = EntityUtils.toString(response.getEntity(), charset);
            return Jsoup.parse(html);
        } catch (Exception var9) {
            var9.printStackTrace();
            return (Document)dom;
        }
    }

    public Document getDom(String url, DefaultHttpClient httpClient) {
        return this.getDom(url, httpClient, "GBK");
    }

    private String getUrl(String zone) {
        StringBuilder url = new StringBuilder();
        if (!zone.startsWith("/")) {
            url.append("http://www.tianqihoubao.com/").append(zone);
        }

        url.append("http://www.tianqihoubao.com").append(zone);
        return url.toString();
    }

    public Set<String> getLinks(Document dom, String domSekector, String cssSelector, int type) {
        Set<String> links = new HashSet();
        Elements es = this.getElements(dom, domSekector, cssSelector);
        if (null == es) {
            LOG.info("Get Links error,element is null. Type is " + type);
            return links;
        } else {
            Iterator iterator = es.iterator();

            while(true) {
                while(iterator.hasNext()) {
                    Element element = (Element)iterator.next();
                    switch(type) {
                        case 1:
                            String province = element.select("a").text();
                            links.add(province + ";" + this.getUrl(element.select("a").attr("href")));
                            break;
                        case 2:
                            String city = element.select("dt").select("a").text();
                            Iterator it = element.select("dd").select("a").iterator();

                            while(it.hasNext()) {
                                Element ele = (Element)it.next();
                                String zone = ele.text();
                                String zoneUrl = ele.attr("href");
                                String zoneId = zoneUrl.substring(zoneUrl.lastIndexOf("/") + 1, zoneUrl.indexOf(".html"));
                                links.add(zoneId + ";" + city + ";" + zone + ";" + this.getUrl(zoneUrl));
                            }
                            break;
                        default:
                            String url = element.attr("href");
                            if (url.contains("lishi")) {
                                links.add(this.getUrl(url));
                            }
                    }
                }

                return links;
            }
        }
    }

    public Elements getElements(Document dom, String domSekector, String cssSelector) {
        Element ele = dom.select(domSekector).first();
        return null == ele ? null : ele.select(cssSelector);
    }
}
