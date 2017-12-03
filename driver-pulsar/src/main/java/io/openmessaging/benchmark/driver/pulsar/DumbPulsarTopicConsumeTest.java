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
import org.apache.pulsar.client.admin.PersistentTopics;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.internal.PersistentTopicsImpl;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.PersistentTopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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

        pulsarClient = PulsarClient.create("pulsar://pulsar.cdp:6650", clientConfiguration);
        pulsarAdmin = new PulsarAdmin(new URL("http://pulsar.cdp:8080"), clientConfiguration);
    }
}

class WriteTopic implements Callable<Exception> {
    private static final Logger log = LoggerFactory.getLogger(WriteTopic.class);
    private static final int PRINT_EVERY_NTH_MESSAGE = 1000;

    private static final SimpleDateFormat formatter = new SimpleDateFormat("yyyyy-mm-dd hh:mm:ss");

    private final int messageId;
    private final String key;
    private final PulsarClient client;
    private final PulsarAdmin adminClient;

    public WriteTopic(int messageId, String key) throws PulsarClientException, MalformedURLException {
        this.messageId = messageId;
        this.key = key;
        client = CreateClients.pulsarClient;
        adminClient = CreateClients.pulsarAdmin;
    }

    @Override
    public Exception call() throws Exception {
        try {
            String topic = String.format("persistent://prop-us-west-1a-noproxy/us-west-1a-noproxy/ns/%s-topic-%s", key, messageId);
            /*try {
                adminClient.persistentTopics().delete(topic);
            } catch (PulsarAdminException.NotFoundException notFound) {
                // nop
            }*/
            //adminClient.persistentTopics().createPartitionedTopic(topic, 2);
            Producer producer = client.createProducer(topic);
            for (int i = 0; i < 200; i++) {
                producer.send(MessageBuilder.create()
                        .setKey(String.valueOf(messageId))
                        .setContent(ByteBuffer.allocate(4).putInt(i).array()).build());
            }
            if (messageId % PRINT_EVERY_NTH_MESSAGE == 0) {
                log.info("{}: Produced message {}", formatter.format(new Date()), messageId);
            }
            producer.close();
            return null;
        } catch (Exception e) {
            return e;
        }
    }
}

class ConsumeTopic implements Callable<Exception> {
    private static final Logger log = LoggerFactory.getLogger(ConsumeTopic.class);
    private static final int PRINT_EVERY_NTH_MESSAGE = 1;

    private final int messageId;
    private final long startTime;
    private final String key;
    private final PulsarClient client;
    private final PulsarAdmin adminClient;

    public ConsumeTopic(int messageId, long startTime, String key) throws PulsarClientException, MalformedURLException {
        this.messageId = messageId;
        this.startTime = startTime;
        this.key = key;
        client = CreateClients.pulsarClient;
        adminClient = CreateClients.pulsarAdmin;
    }

    @Override
    public Exception call() throws Exception {
        try {
            String topic = String.format("persistent://prop-us-west-1a-noproxy/us-west-1a-noproxy/ns/%s-topic-%s", key, messageId);

            // Wait for topic to be created
            while (true) {
                try {
                    PersistentTopicStats stats = adminClient.persistentTopics().getStats(topic);
                    log.info("Topic created {} (size: {}), next step, subscribe", topic, stats.storageSize);
                    break;
                } catch (Exception e) {
                    log.info("Topic not created yet, checking again in 1 second");
                    Thread.sleep(1000);
                }
            }

            ConsumerConfiguration conf = new ConsumerConfiguration();
            long consumerCreateTimeStart = System.currentTimeMillis();
            conf.setSubscriptionType(SubscriptionType.Exclusive);
            Consumer consumer = client.subscribe(topic, "sub-" + messageId, conf);
            long consumerCreateTimeEnd = System.currentTimeMillis();
            log.info("Started subscribe to topic");
            Thread.sleep(1000);

            while (true) {
                AtomicInteger numMessages = new AtomicInteger(0);
                //
                long consumerReceiveTimeStart = System.currentTimeMillis();

                Message message = consumer.receive(100, TimeUnit.MILLISECONDS);
                if (message == null) {
                    log.warn("Got null message, expected at least 1 message. Stats: \n" +
                                    "\tLedger entries -> {}\n" +
                                    "\tcursorMap -> {}\n",
                            adminClient.persistentTopics().getInternalStats(topic).currentLedgerEntries,
                            adminClient.persistentTopics().getInternalStats(topic).cursors.values().stream()
                                    .map(stats -> String.format("State: %s", stats.state))
                                    .collect(Collectors.joining(", ")));
                            adminClient.persistentTopics().resetCursor(topic, "sub-" + messageId, -1);
                            Thread.sleep(1000);
                    continue;
                }
                numMessages.incrementAndGet();
                try {
                    consumer.acknowledge(message);
                } catch (Exception e) {
                    if (e.getMessage().contains("Not connected to broker. State: Connecting")) {
                        log.warn("Waiting for consumer to reconnect...", e);
                        Thread.sleep(1000);
                    }
                }

                long consumerReceiveTimeEnd = System.currentTimeMillis();
                if (messageId % PRINT_EVERY_NTH_MESSAGE == 0) {
                    log.info("Consumed message {}, took {} ms to subscribe, " +
                                    "took {} ms to consume {} messages, topic: {}",
                            new BigInteger(message.getData()).intValue(),
                            consumerCreateTimeEnd - consumerCreateTimeStart,
                            consumerReceiveTimeEnd - consumerReceiveTimeStart, numMessages,
                            topic);
                    Thread.sleep(1000);
                }
            }
        } catch (Exception e) {
            return new Exception("Failed on message id " + messageId, e);
        }

    }
}

public class DumbPulsarTopicConsumeTest {
    private static final Logger log = LoggerFactory.getLogger(DumbPulsarTopicConsumeTest.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, MalformedURLException, PulsarClientException {
        CreateClients.setClients();

        String key = "default";
        Integer numConcurrentConsumers = 15;
        if (args.length >= 1) {
            log.info("Arg passed: {}", args[0]);
            key = args[0];
            if (args.length >= 2) {
                numConcurrentConsumers = Integer.valueOf(args[1]);
            }
        }
        ExecutorService writeTopics = Executors.newFixedThreadPool(15);
        ExecutorService consumeTopics = Executors.newFixedThreadPool(numConcurrentConsumers);

        int numTopics = numConcurrentConsumers;

        BlockingQueue<Future<Exception>> consumerFutures = new ArrayBlockingQueue<>(numConcurrentConsumers);

        BlockingQueue<Future<Exception>> writeFutures = new ArrayBlockingQueue<>(100);
        for (int topic = 1; topic <= numTopics; topic++) {
            writeFutures.put(writeTopics.submit(new WriteTopic(topic, key)));
            if (writeFutures.size() >= 100) {
                log.info("Produced 10k topics, ensuring success before proceeding...");
                clearQueue(writeFutures);
            }
            consumerFutures.put(consumeTopics.submit(new ConsumeTopic(topic, 0, key)));
            if (consumerFutures.size() >= numConcurrentConsumers) {
                //log.info("hmmmm {}", topicNum);
                //clearQueue(consumerFutures);
            }
        }

        writeTopics.shutdown();
        try {
            clearQueue(consumerFutures);
        } finally {
            consumeTopics.shutdownNow();
        }

    }

    private static int clearQueue(BlockingQueue<Future<Exception>> futures) throws InterruptedException, ExecutionException {
        int runningTally = 0;
        while (!futures.isEmpty()) {
            if (futures.peek().isDone()) {
                Future<Exception> f = futures.take();
                log.info("Waiting for {} to close", f.toString());
                Exception e = f.get();
                if (e != null) {
                    log.error("Fatal error:", e);
                    throw new ExecutionException(e);
                }

                runningTally++;
            }
        }
        return runningTally;
    }
}
