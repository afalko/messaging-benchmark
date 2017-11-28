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

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

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
        String topic = "persistent://prop-us-west-1a-noproxy/us-west-1a-noproxy/ns/topic-" + messageId;
        adminClient.persistentTopics().createPartitionedTopic(topic, 1);
        Producer producer = client.createProducer(topic);
        producer.send(new byte[10]);
        if (messageId % 1000 == 0) {
            log.info("{}: Produced message {}", formatter.format(new Date()), messageId);
        }
        return null;
    }
}

class ConsumeTopic implements Callable {
    private static final Logger log = LoggerFactory.getLogger(ConsumeTopic.class);

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
        String topic = "persistent://prop-us-west-1a-noproxy/us-west-1a-noproxy/ns/topic-" + messageId;

        adminClient.persistentTopics().resetCursor(topic, "sub-" + messageId, startTime);

        ConsumerConfiguration conf = new ConsumerConfiguration();
        long consumerCreateTimeStart = System.currentTimeMillis();
        conf.setSubscriptionType(SubscriptionType.Failover);
        Consumer consumer = client.subscribe(topic, "sub-" + messageId, conf);
        long consumerCreateTimeEnd = System.currentTimeMillis();
        consumer.receive();
        long consumerReceiveTimeEnd = System.currentTimeMillis();
        if (messageId % 1000 == 0) {
            log.info("Consumed message {}, took {} ms to subscribe, took {} ms to consume first message",
                    messageId, consumerCreateTimeEnd - consumerCreateTimeStart, consumerReceiveTimeEnd - consumerCreateTimeEnd);
        }
        return null;
    }
}

public class DumbPulsarTopicConsumeTest {
    private static final Logger log = LoggerFactory.getLogger(DumbPulsarTopicConsumeTest.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, MalformedURLException, PulsarClientException {
        CreateClients.setClients();

        long startTime = System.currentTimeMillis();

        ExecutorService writeTopics = Executors.newFixedThreadPool(15);
        int numTopics = 10000;
        BlockingQueue<Future> futures = new ArrayBlockingQueue<>(10000);
        for (int i = 0; i < numTopics; i++) {
            futures.put(writeTopics.submit(new WriteTopic(i)));
        }
        int runningTally = 0;
        while (!futures.isEmpty()) {
            if (futures.peek().isDone()) {
                futures.remove();
                runningTally++;
                log.info("Created topic #{}", runningTally);
            }
        }
        writeTopics.shutdown();

        ExecutorService consumeTopics = Executors.newFixedThreadPool(15);
        for (int i = 0; i < numTopics; i++) {
            futures.put(consumeTopics.submit(new ConsumeTopic(i, startTime)));
        }

        runningTally = 0;
        while (!futures.isEmpty()) {
            if (futures.peek().isDone()) {
                futures.remove();
                runningTally++;
                log.info("Consumed from topic #{}", runningTally);
            }
        }

        consumeTopics.shutdown();
    }
}
