package org.atlasapi.messaging.v3;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import javax.jms.ConnectionFactory;

import org.atlasapi.messaging.worker.v3.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.adapter.MessageListenerAdapter;

import com.google.common.base.Strings;

public final class QueueFactory {

    private static final Logger log = LoggerFactory.getLogger(QueueFactory.class);
    
    private final String system;
    private final ConnectionFactory cf;

    public QueueFactory(ConnectionFactory cf, String system) {
        this.cf = checkNotNull(cf);
        checkArgument(!Strings.isNullOrEmpty(system));
        this.system = system;
    }

    private String virtualTopicProducer(String name) {
        return String.format("VirtualTopic.%s.%s", system, name);
    }
    
    private String virtualTopicConsumer(String consumer, String producer) {
        return String.format("Consumer.%s.VirtualTopic.%s.%s", consumer, system, producer);
    }
    
    private String replayDestination(String name) {
        return String.format("%s.Replay.%s", name, system);
    }
    
    public DefaultMessageListenerContainer makeVirtualTopicConsumer(Worker worker, String consumer, String producer, int consumers, int maxConsumers) {
        return makeContainer(worker, virtualTopicConsumer(consumer, producer), consumers, maxConsumers);
    }

    public DefaultMessageListenerContainer makeReplayContainer(Worker worker, String name, int consumers, int maxConsumers) {
        return makeContainer(worker, replayDestination(name), consumers, maxConsumers);
    }
    
    public DefaultMessageListenerContainer makeContainer(Worker worker, String destination, int consumers, int maxConsumers) {
        log.info("Reading {} with {}", destination, worker.getClass().getSimpleName());
        MessageListenerAdapter adapter = new MessageListenerAdapter(worker);
        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();

        adapter.setDefaultListenerMethod("onMessage");
        container.setConnectionFactory(cf);
        container.setDestinationName(destination);
        container.setConcurrentConsumers(consumers);
        container.setMaxConcurrentConsumers(maxConsumers);
        container.setMessageListener(adapter);

        return container;
    }
    
    public JmsTemplate makeVirtualTopicProducer(String producerName) {
        String destination = this.virtualTopicProducer(producerName);
        log.info("Writing {}", destination);
        JmsTemplate jmsTemplate = new JmsTemplate(cf);
        jmsTemplate.setPubSubDomain(true);
        jmsTemplate.setDefaultDestinationName(destination);
        return jmsTemplate;
    }

    public DefaultMessageListenerContainer noopContainer() {
        DefaultMessageListenerContainer container = new DefaultMessageListenerContainer();

        container.setConnectionFactory(cf);
        container.setDestinationName("do.not.write.to.this.queue");

        return container;
    }
    
}