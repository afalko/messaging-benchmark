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

import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.apache.pulsar.client.admin.PersistentTopics;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PulsarBenchmarkConsumer implements BenchmarkConsumer {

    private final Consumer consumer;
    private final PersistentTopics persistentTopics;
    private final ExecutorService executor;

    public PulsarBenchmarkConsumer(Consumer consumer, PersistentTopics persistentTopics) {
        this.consumer = consumer;
        this.persistentTopics = persistentTopics;
        executor = Executors.newSingleThreadExecutor();
    }

    @Override
    public void close() throws Exception {
        consumer.close();
    }

    @Override
    public CompletableFuture<Void> receiveAsync(ConsumerCallback callback, final boolean testCompleted) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        this.executor.execute(() -> {
            try {
                while (!testCompleted) {
                    Message message = consumer.receive();
                    callback.messageReceived(message.getData(), System.nanoTime());
                    if (consumer.hasReachedEndOfTopic()) {
                        //persistentTopics.resetCursor();
                    }
                    /*conf.setMessageListener((consumer, msg) -> {
                        consumerCallback.messageReceived(msg.getData(), System.nanoTime());
                        consumer.acknowledgeAsync(msg);
                    });*/
                    /*if (consumer.hasReachedEndOfTopic()) {
                        consumer.getSubscription()
                    }
                    Message message = consumer.receive();
                    for (ConsumerRecord<byte[], byte[]> record : records) {
                        callback.messageReceived(record.value(), System.nanoTime());

                        offsetMap.put(new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset()));
                    }

                    if (!offsetMap.isEmpty()) {
                        consumer.commitSync(offsetMap);
                    }*/
                }

                consumer.unsubscribe();
                consumer.close();
            } catch (Throwable e) {
                future.completeExceptionally(e);
            }
            future.complete(null);
        });
        return future;
    }
}
