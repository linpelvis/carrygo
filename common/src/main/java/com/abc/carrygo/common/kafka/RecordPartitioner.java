package com.abc.carrygo.common.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Created by plin on 9/29/16.
 */
public class RecordPartitioner<T> implements Partitioner {

    public RecordPartitioner(VerifiableProperties verifiableProperties) {
    }

    @Override
    public int partition(Object key, int numPartitions) {
        try {
            int partitionNum = Integer.parseInt((String) key);
            return Math.abs(partitionNum % numPartitions);
        } catch (Exception e) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }
}
