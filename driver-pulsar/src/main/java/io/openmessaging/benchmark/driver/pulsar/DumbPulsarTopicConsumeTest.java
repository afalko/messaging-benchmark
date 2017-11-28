package io.openmessaging.benchmark.driver.pulsar;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.*;

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

        pulsarClient = PulsarClient.create("pulsar://localhost:6650", clientConfiguration);
        pulsarAdmin = new PulsarAdmin(new URL("http://localhost:8080"), clientConfiguration);
    }
}

class WriteTopic implements Callable {
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
            System.out.println(String.format("%s: Produced message %s", formatter.format(new Date()), messageId));
        }
        return null;
    }
}

class ConsumeTopic implements Callable {
    private final int messageId;
    private final PulsarClient client;
    private final PulsarAdmin adminClient;

    public ConsumeTopic(int messageId) throws PulsarClientException, MalformedURLException {
        this.messageId = messageId;
        client = CreateClients.pulsarClient;
        adminClient = CreateClients.pulsarAdmin;
    }

    @Override
    public Object call() throws Exception {
        String topic = "persistent://prop-us-west-1a-noproxy/us-west-1a-noproxy/ns/topic-" + messageId;

        ConsumerConfiguration conf = new ConsumerConfiguration();
        long consumerCreateTimeStart = System.currentTimeMillis();
        conf.setSubscriptionType(SubscriptionType.Failover);
        Consumer consumer = client.subscribe(topic, "sub-" + messageId, conf);
        long consumerCreateTimeEnd = System.currentTimeMillis();
        consumer.receive();
        long consumerReceiveTimeEnd = System.currentTimeMillis();
        if (messageId % 1000 == 0) {
            System.out.println(String.format("Consumed message %s, took %s ms to subscribe, took %s ms to consume first message",
                    messageId, consumerCreateTimeEnd - consumerCreateTimeStart, consumerReceiveTimeEnd - consumerCreateTimeEnd));
        }
        return null;
    }
}

public class DumbPulsarTopicConsumeTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException, MalformedURLException, PulsarClientException {
        CreateClients.setClients();

        ExecutorService writeTopics = Executors.newFixedThreadPool(15);
        int numTopics = 10000;
        BlockingQueue<Future> futures = new ArrayBlockingQueue<>(10000);
        for (int i = 0; i < numTopics; i++) {
            futures.put(writeTopics.submit(new WriteTopic(i)));
        }
        while (!futures.isEmpty()) {
            if (futures.peek().isDone()) {
                futures.remove();
            }
        }
        writeTopics.shutdown();

        ExecutorService consumeTopics = Executors.newFixedThreadPool(15);
        for (int i = 0; i < numTopics; i++) {
            futures.put(consumeTopics.submit(new ConsumeTopic(i)));
        }

        while (!futures.isEmpty()) {
            if (futures.peek().isDone()) {
                futures.remove();
            }
        }

        consumeTopics.shutdown();
    }
}
