package com.cuteximi.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @program: weathermrs
 * @description: Kafka 的配置
 * @author: TSL
 * @create: 2018-11-30 22:34
 **/
public class KafkaProperties {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaProperties.class);
    public static final String TOPIC = "Test";
    private static Properties serverProps = new Properties();
    private static Properties producerProps = new Properties();
    private static Properties consumerProps = new Properties();
    private static Properties clientProps = new Properties();
    private static KafkaProperties instance = null;
    private static String KafkaPath = "/opt/client/Kafka/kafka";

    private KafkaProperties() {
        String filePath = KafkaPath + File.separator + "config" + File.separator;

        try {
            File proFile = new File(filePath + "producer.properties");
            if (proFile.exists()) {
                producerProps.load(new FileInputStream(filePath + "producer.properties"));
            }

            File conFile = new File(filePath + "producer.properties");
            if (conFile.exists()) {
                consumerProps.load(new FileInputStream(filePath + "consumer.properties"));
            }

            File serFile = new File(filePath + "server.properties");
            if (serFile.exists()) {
                serverProps.load(new FileInputStream(filePath + "server.properties"));
            }

            File cliFile = new File(filePath + "client.properties");
            if (cliFile.exists()) {
                clientProps.load(new FileInputStream(filePath + "client.properties"));
            }
        } catch (IOException var6) {
            LOG.info("The Exception occured.", var6);
        }

    }

    public static synchronized KafkaProperties getInstance() {
        if (null == instance) {
            instance = new KafkaProperties();
        }

        return instance;
    }

    public String getValues(String key, String defValue) {
        String rtValue = null;
        if (null == key) {
            LOG.error("key is null");
        } else {
            rtValue = this.getPropertiesValue(key);
        }

        if (null == rtValue) {
            LOG.warn("KafkaProperties.getValues return null, key is " + key);
            rtValue = defValue;
        }

        LOG.info("KafkaProperties.getValues: key is " + key + "; Value is " + rtValue);
        return rtValue;
    }

    private String getPropertiesValue(String key) {
        String rtValue = serverProps.getProperty(key);
        if (null == rtValue) {
            rtValue = producerProps.getProperty(key);
        }

        if (null == rtValue) {
            rtValue = consumerProps.getProperty(key);
        }

        if (null == rtValue) {
            rtValue = clientProps.getProperty(key);
        }

        return rtValue;
    }
}