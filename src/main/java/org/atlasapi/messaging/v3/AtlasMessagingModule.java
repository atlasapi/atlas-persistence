package org.atlasapi.messaging.v3;

import javax.jms.ConnectionFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Primary;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;

@Configuration
public class AtlasMessagingModule {
    
    @Value("${messaging.broker.url}")
    private String brokerUrl;
    @Value("${messaging.producer.window.size}")
    private String producerWindowSize;
    @Value("${messaging.system}")
    private String messagingSystem;
    @Value("${messaging.destination.content.changes}")
    private String contentChanges;
    @Value("${messaging.destination.topics.changes}")
    private String topicChanges;
    @Value("${messaging.destination.equiv.changes}")
    private String equivChanges;
    
    public AtlasMessagingModule(String brokerUrl, String messagingSystem) {
        this.brokerUrl = brokerUrl;
        this.messagingSystem = messagingSystem;
    }

    public AtlasMessagingModule() { }
    
    @Bean @Primary
    public QueueFactory queueHelper() {
        return new QueueFactory(producerFactory(), consumerFactory(), messagingSystem);
    }
    
    @Bean
    @Lazy(true)
    public ConnectionFactory consumerFactory() {
        ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        return cf;
    }
    
    @Bean
    @Lazy(true)
    public ConnectionFactory producerFactory() {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        cf.setProducerWindowSize(Integer.parseInt(producerWindowSize));
        ConnectionFactory cachingConnectionFactory = new CachingConnectionFactory(cf);
        return cachingConnectionFactory;
    }
    
    @Bean
    @Lazy(true)
    public JmsTemplate contentChanges() {
        return queueHelper().makeVirtualTopicProducer(contentChanges);
    }
    
    @Bean
    @Lazy(true)
    public JmsTemplate topicChanges() {
        return queueHelper().makeVirtualTopicProducer(topicChanges);
    }

    @Bean
    @Lazy(true)
    public JmsTemplate equivChanges() {
        return queueHelper().makeVirtualTopicProducer(equivChanges);
    }

    public JmsTemplate equivAssertions() {
        return null;
    }
    
}