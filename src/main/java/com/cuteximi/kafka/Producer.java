package com.cuteximi.kafka;

import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @program: weathermrs
 * @description: 生产者
 * @author: TSL
 * @create: 2018-11-30 22:34
 **/
public class Producer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
    private final kafka.javaapi.producer.Producer<String, String> producer;
    private final String topic;
    private final Properties props = new Properties();
    private final String producerType = "producer.type";
    private final String partitionerClass = "partitioner.class";
    private final String serializerClass = "serializer.class";
    private final String metadataBrokerList = "metadata.broker.list";
    private final String bootstrapServers = "bootstrap.servers";

    public Producer(String topicName) {
        this.props.put("producer.type", "sync");
        this.props.put("partitioner.class", "com.huawei.mrs.kafka.SimplePartitioner");
        this.props.put("serializer.class", "kafka.serializer.StringEncoder");
        this.props.put("metadata.broker.list", KafkaProperties.getInstance().getValues("bootstrap.servers", "localhost:9092"));
        this.producer = new kafka.javaapi.producer.Producer(new ProducerConfig(this.props));
        this.topic = topicName;
    }

    public void run(String line) {
        LOG.info("Producer: start.");
        String[] fields = line.split(",", -1);
        String key = fields[0];
        this.producer.send(new KeyedMessage(this.topic, key, line));
        LOG.info("Producer: send message to " + this.topic);
    }
}