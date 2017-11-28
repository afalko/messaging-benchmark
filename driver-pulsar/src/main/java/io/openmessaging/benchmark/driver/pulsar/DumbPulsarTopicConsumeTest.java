/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.pulsar;

import com.google.common.primitives.Bytes;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.*;
import java.util.stream.Collectors;

class CreateClients {
    static PulsarClient pulsarClient;
    static PulsarAdmin pulsarAdmin;

    public static void setClients() throws PulsarClientException, MalformedURLException {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setIoThreads(1);
        clientConfiguration.setConnectionsPerBroker(1);

        // Disable internal stats since we're already collecting in the framework
        clientConfiguration.setStatsInterval(0, TimeUnit.SECONDS);

        pulsarClient = PulsarClient.create("pulsar://broker:6650", clientConfiguration);
        pulsarAdmin = new PulsarAdmin(new URL("http://broker:8080"), clientConfiguration);
    }
}

class WriteTopic implements Callable {
    private static final Logger log = LoggerFactory.getLogger(WriteTopic.class);
    private static final int PRINT_EVERY_NTH_MESSAGE = 1;

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyy-mm-dd hh:mm:ss");

    private final int messageId;
    private final PulsarClient client;
    private final PulsarAdmin adminClient;

    public WriteTopic(int messageId) throws PulsarClientException, MalformedURLException {
        this.messageId = messageId;
        client = CreateClients.pulsarClient;
        adminClient = CreateClients.pulsarAdmin;
    }

    @Override
    public Object call() throws Exception {
        String topic = "persistent://prop-us-west-1a-noproxy/us-west-1a-noproxy/ns/ftopic-" + messageId;
        try {
            adminClient.persistentTopics().delete(topic);
        } catch (PulsarAdminException.NotFoundException notFound) {
            // nop
        }
        //adminClient.persistentTopics().createPartitionedTopic(topic, 2);
        Producer producer = client.createProducer(topic);
        producer.send(Bytes.toArray(Collections.singleton(messageId)));
        producer.send(Bytes.toArray(Collections.singleton(messageId)));
        producer.send(Bytes.toArray(Collections.singleton(messageId)));
        if (messageId % PRINT_EVERY_NTH_MESSAGE == 0) {
            log.info("{}: Produced message {}", formatter.format(new Date()), messageId);
        }
        // The only way to know that we are at the end of message is to terminate the topic
        Future terminateCall = adminClient.persistentTopics().terminateTopicAsync(topic);
        terminateCall.get();
        producer.close();
        return null;
    }
}

class ConsumeTopic implements Callable {
    private static final Logger log = LoggerFactory.getLogger(ConsumeTopic.class);
    private static final int PRINT_EVERY_NTH_MESSAGE = 1;

    private final int messageId;
    private final long startTime;
    private final PulsarClient client;
    private final PulsarAdmin adminClient;

    public ConsumeTopic(int messageId, long startTime) throws PulsarClientException, MalformedURLException {
        this.messageId = messageId;
        this.startTime = startTime;
        client = CreateClients.pulsarClient;
        adminClient = CreateClients.pulsarAdmin;
    }

    @Override
    public Object call() throws Exception {
        String topic = "persistent://prop-us-west-1a-noproxy/us-west-1a-noproxy/ns/ftopic-" + messageId;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        long consumerCreateTimeStart = System.currentTimeMillis();
        conf.setSubscriptionType(SubscriptionType.Exclusive);
        Consumer consumer = client.subscribe(topic, "sub-" + messageId, conf);
        adminClient.persistentTopics().resetCursor(topic, "sub-" + messageId, 0);

        long consumerCreateTimeEnd = System.currentTimeMillis();
        int numMessages = 0;
        while (!consumer.hasReachedEndOfTopic()) {
            Message message = consumer.receive(100, TimeUnit.MILLISECONDS);
            if (message == null) {
                log.warn("Got null message, expected at least 1 message. Stats: \n" +
                                "\tLedger entries -> {}\n" +
                                "\tcursorMap -> {}\n",
                        adminClient.persistentTopics().getInternalStats(topic).currentLedgerEntries,
                        adminClient.persistentTopics().getInternalStats(topic).cursors.values().stream()
                                .map(stats -> String.format("State: %s", stats.state))
                                .collect(Collectors.joining(", ")));
                continue;
            }
            numMessages++;
            consumer.acknowledge(message);
            break;
        }
        long consumerReceiveTimeEnd = System.currentTimeMillis();
        if (numMessages < 1) {
            consumer.unsubscribe();
            consumer.close();
            throw new RuntimeException("No messages consumed");
        }
        if (messageId % PRINT_EVERY_NTH_MESSAGE == 0) {
            log.info("Consumed message {}, took {} ms to subscribe, took {} ms to consume {} messages",
                    messageId, consumerCreateTimeEnd - consumerCreateTimeStart,
                    consumerReceiveTimeEnd - consumerCreateTimeEnd, numMessages);
        }
        consumer.unsubscribe();
        consumer.close();
        return null;
        /*ReaderConfiguration readerConfiguration = new ReaderConfiguration();
        readerConfiguration.setReceiverQueueSize(1);
        long consumerCreateTimeStart = System.currentTimeMillis();
        readerConfiguration.setReaderListener((ReaderListener) (reader, msg) -> {
            long consumerCreateTimeEnd = System.currentTimeMillis();
            long consumerReceiveTimeEnd = System.currentTimeMillis();
            if (messageId % PRINT_EVERY_NTH_MESSAGE == 0) {
                log.info("Consumed message {}, took {} ms to subscribe, took {} ms to consume first message",
                        msg.getMessageId(), consumerCreateTimeEnd - consumerCreateTimeStart, consumerReceiveTimeEnd - consumerCreateTimeEnd);
            }
        });
        Reader reader = client.createReader(topic, MessageId.earliest, readerConfiguration);
        while (!reader.hasReachedEndOfTopic()) {
            reader.readNext();
        }

        //Message message = reader.readNextAsync().get();
        /*long consumerReceiveTimeEnd = System.currentTimeMillis();
        if (messageId % PRINT_EVERY_NTH_MESSAGE == 0) {
            log.info("Consumed message {}, took {} ms to subscribe, took {} ms to consume first message",
                    message.getMessageId(), consumerCreateTimeEnd - consumerCreateTimeStart, consumerReceiveTimeEnd - consumerCreateTimeEnd);
        }
        return null;*/
    }
}

public class DumbPulsarTopicConsumeTest {
    private static final Logger log = LoggerFactory.getLogger(DumbPulsarTopicConsumeTest.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, MalformedURLException, PulsarClientException {
        CreateClients.setClients();

        long startTime = System.currentTimeMillis();

        /*final List<String> allTopics = new ArrayList();
        CreateClients.pulsarAdmin.namespaces().getNamespaces("prop-us-west-1a-noproxy").forEach(ns -> {
            try {
                //log.info("All topics for ns {}: {}", ns, CreateClients.pulsarAdmin.persistentTopics().getList(ns));
                allTopics.addAll(CreateClients.pulsarAdmin.persistentTopics().getList(ns));
            } catch (PulsarAdminException e) {
                log.error("Err", e);
            }
        });

        ReaderConfiguration readerConfiguration = new ReaderConfiguration();
        readerConfiguration.setReceiverQueueSize(1);
        allTopics.forEach(topic -> {
            Reader reader = null;
            try {
                reader = CreateClients.pulsarClient.createReader(topic, MessageId.earliest, readerConfiguration);
            } catch (PulsarClientException e) {
                log.error("Err", e);
            }
            assert reader != null;
            while (!CreateClients.pulsarAdmin.persistentTopics().getInternalStats(t).) {
                try {
                    Message message = reader.readNext();
                    log.info("Message {}", message.getData());
                } catch (PulsarClientException e) {
                    log.error("Err", e);
                }
            }
        });*/
        ExecutorService writeTopics = Executors.newFixedThreadPool(15);
        int runningTally = 0;

        int numTopics = 1;

        BlockingQueue<Future> consumerFutures = new ArrayBlockingQueue<>(10000);

        BlockingQueue<Future> writeFutures = new ArrayBlockingQueue<>(10000);
        runningTally = 0;
        for (int i = 0; i < numTopics; i++) {
            writeFutures.put(writeTopics.submit(new WriteTopic(i)));
        }
        while (!writeFutures.isEmpty()) {
            if (writeFutures.peek().isDone()) {
                writeFutures.take().get();
                runningTally++;
                //log.info("Created topic #{}", runningTally);
            }
        }

        writeTopics.shutdown();

        ExecutorService consumeTopics = Executors.newFixedThreadPool(15);
        for (int i = 0; i < numTopics; i++) {
            consumerFutures.put(consumeTopics.submit(new ConsumeTopic(i, 0)));
        }

        runningTally = 0;
        while (!consumerFutures.isEmpty()) {
            if (consumerFutures.peek().isDone()) {
                try {
                    consumerFutures.take().get();
                } catch (Exception e) {
                    consumeTopics.awaitTermination(1, TimeUnit.SECONDS);
                    throw e;
                }
                runningTally++;
                //log.info("Consumed from topic #{}", runningTally);
            }
        }

        consumeTopics.shutdownNow();
    }
}
