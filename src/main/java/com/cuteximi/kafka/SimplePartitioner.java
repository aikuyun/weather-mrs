package com.cuteximi.kafka;

/**
 * @program: weathermrs
 * @description: kafka 简单分区
 * @author: TSL
 * @create: 2018-11-30 22:34
 **/
public class SimplePartitioner {
    public SimplePartitioner(VerifiableProperties props) {
    }

    public int partition(Object key, int numPartitions) {
        int partition = false;
        String partitionKey = (String) key;

        int partition;
        try {
            partition = Integer.parseInt(partitionKey) % numPartitions;
        } catch (NumberFormatException var6) {
            partition = 0;
        }

        return partition;
    }
}