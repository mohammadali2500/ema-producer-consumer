package com.example.fx.consumer.control;

import com.example.fx.consumer.config.ConsumerControlProperties;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumerLifecycle {
    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerControlProperties properties;

    public KafkaConsumerLifecycle(
            KafkaListenerEndpointRegistry registry,
            ConsumerControlProperties properties) {
        this.registry = registry;
        this.properties = properties;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void applyVaultStartupFlag() {
        if (properties.autoStart()) {
            start();
        }
    }

    public synchronized void start() {
        MessageListenerContainer container = container();
        if (!container.isRunning()) {
            container.start();
        }
    }

    public synchronized void stop() {
        MessageListenerContainer container = container();
        if (container.isRunning()) {
            container.stop();
        }
    }

    public boolean isRunning() {
        MessageListenerContainer container = registry.getListenerContainer(properties.listenerId());
        return container != null && container.isRunning();
    }

    private MessageListenerContainer container() {
        MessageListenerContainer container = registry.getListenerContainer(properties.listenerId());
        if (container == null) {
            throw new IllegalStateException("Kafka listener not registered: " + properties.listenerId());
        }
        return container;
    }
}
