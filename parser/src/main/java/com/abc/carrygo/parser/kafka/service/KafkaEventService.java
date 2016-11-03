package com.abc.carrygo.parser.kafka.service;

import com.abc.carrygo.common.factory.ListeningExecutorFactory;
import com.abc.carrygo.common.kafka.KafkaAddress;
import com.abc.carrygo.common.kafka.KafkaConnectors;
import com.abc.carrygo.parser.EventService;
import com.abc.carrygo.parser.common.entity.Event;
import com.abc.carrygo.parser.kafka.PublishCount;
import com.abc.carrygo.parser.kafka.manager.KafkaFeedManager;
import com.abc.carrygo.parser.kafka.parse.KafkaEntryParser;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;


public class KafkaEventService extends EventService {
    private static final Logger log = LoggerFactory.getLogger(KafkaEventService.class);
    private static final Splitter SPLITTER = Splitter.on(",").omitEmptyStrings();
    private static final String PUBLISH_EXECUTOR_WORK_NAME = "PUBLISH-WORK";
    private static final String PUBLISH_EXECUTOR_LISTENER_NAME = "PUBLISH-LISTENER";
    private static final int EXECUTOR_NUM_THREAD = 1;
    private static int default_threads;
    private static CountDownLatch latch = new CountDownLatch(1);
    private volatile boolean publish_running = false;
    private KafkaFeedProcessor processor;
    private ListeningExecutorFactory service;
    private List<String> topics;

    public KafkaEventService(String servers,
                             String destination,
                             KafkaAddress kafkaAddress,
                             String kafkaTopic) {
        super(destination, CanalConnectors.newClusterConnector(
                servers,
                destination,
                "",
                ""));

        init(kafkaAddress, kafkaTopic);
    }

    public void init(KafkaAddress kafkaAddress, String kafkaTopic) {
        this.topics = SPLITTER.splitToList(kafkaTopic);


        default_threads = Math.max(topics.size(),
                Runtime.getRuntime().availableProcessors() * 2);

        Producer producer = KafkaConnectors.newProducer(kafkaAddress);
        KafkaEventHandler eventHandler = new KafkaEventHandler(producer);

        KafkaFeedProcessorBuilder builder = new KafkaFeedProcessorBuilder();
        builder
                .setEventHandler(eventHandler)
                .setFeedManager(new KafkaFeedManager())
                .setEventFactory(new KafkaFeedProcessor(topics))
                .setListeningExecutorFactory(new ListeningExecutorFactory(default_threads, PUBLISH_EXECUTOR_WORK_NAME));
        processor = builder.build(topics);
    }

    public void startPublish() {
        service = new ListeningExecutorFactory(EXECUTOR_NUM_THREAD, PUBLISH_EXECUTOR_LISTENER_NAME);
        final ListenableFuture future = service.submit(new Callable() {
            @Override
            public Boolean call() throws Exception {
                boolean flag = false;

//                do {
//                    flag = PublishCount.check();
//
//                } while (!flag);

                if (!flag) {
                    latch.await();
                }

                publish_running = true;

                return Boolean.valueOf(publish_running);
            }
        });

        Futures.addCallback(future, new FutureCallback() {
            @Override
            public void onSuccess(Object result) {
                Boolean b = (Boolean) result;
                if (b.booleanValue()) {
                    processor.start();
                } else {
                    throw new RuntimeException(new Throwable("Publish failed."));
                }

                service.shutdown();
            }

            public void onFailure(Throwable thrown) {
                throw new RuntimeException(thrown);
            }
        });
    }


    public void stopPublish() {
        if (!publish_running) {
            return;
        }
        publish_running = false;

        processor.stop();
    }


    protected void parseAndPublish(Message message, long batchId) {
        try {
            int size = message.getEntries().size();
            if (batchId != -1 && size != 0) {
                log.info("parseAndPublish, batchId:{}", batchId);

                Map<String, List<Event>> eventFeedMaps = KafkaEntryParser.parseRowData(
                        message.getEntries(),
                        topics);

                // parse event && put event to queue
                processor.extract(batchId, eventFeedMaps);

                if (PublishCount.check() && !publish_running) {
                    latch.countDown();
                }
            }

            connector.ack(batchId);
        } catch (Exception e) {
            long publishBatchId = PublishCount.get();
            if (publishBatchId < batchId) {
                log.error("origin batchId:{}", batchId);

                batchId = publishBatchId;
            }

            log.error("rollback msg, batchId:{}", batchId, e);
            connector.rollback(batchId);
        }
    }
}
