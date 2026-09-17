package com.example.fx.producer.control;

import com.example.fx.producer.config.RefinitivProperties;
import com.example.fx.producer.ema.EmaConnectionStatus;
import com.example.fx.producer.ema.EmaConsumerFactory;
import com.example.fx.producer.ema.RefinitivQuoteClient;
import com.example.fx.producer.kafka.QuotePublisher;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerConfig;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

@Component
public class RefinitivLifecycle implements SmartLifecycle {
    private final RefinitivProperties properties;
    private final RefinitivQuoteClient quoteClient;
    private final QuotePublisher quotePublisher;
    private final EmaConnectionStatus connectionStatus;
    private final EmaConsumerFactory consumerFactory;

    private volatile OmmConsumer consumer;

    public RefinitivLifecycle(
            RefinitivProperties properties,
            RefinitivQuoteClient quoteClient,
            QuotePublisher quotePublisher,
            EmaConnectionStatus connectionStatus,
            EmaConsumerFactory consumerFactory) {
        this.properties = properties;
        this.quoteClient = quoteClient;
        this.quotePublisher = quotePublisher;
        this.connectionStatus = connectionStatus;
        this.consumerFactory = consumerFactory;
    }

    @Override
    public synchronized void start() {
        if (consumer != null) {
            return;
        }
        connectionStatus.update(EmaConnectionStatus.State.CONNECTING, "Creating EMA consumer");
        OmmConsumer created = null;
        try {
            created = consumerFactory.create(createConfiguration(), quoteClient);
            quoteClient.registerSubscriptions(created);
            consumer = created;
        } catch (RuntimeException failure) {
            if (created != null) {
                created.uninitialize();
            }
            connectionStatus.update(EmaConnectionStatus.State.FAILED, failure.getMessage());
            throw failure;
        }
    }

    private OmmConsumerConfig createConfiguration() {
        OmmConsumerConfig configuration = EmaFactory.createOmmConsumerConfig()
                .host(properties.host() + ':' + properties.port())
                .username(properties.username())
                .operationModel(OmmConsumerConfig.OperationModel.API_DISPATCH);
        if (properties.password() != null && !properties.password().isBlank()) {
            configuration.password(properties.password());
        }
        return configuration;
    }

    @Override
    public synchronized void stop() {
        OmmConsumer existing = consumer;
        consumer = null;
        if (existing != null) {
            existing.uninitialize();
            quotePublisher.flush();
        }
        connectionStatus.update(EmaConnectionStatus.State.STOPPED, "Stopped locally");
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override public boolean isRunning() { return consumer != null; }
    @Override public boolean isAutoStartup() { return properties.autoStart(); }
    @Override public int getPhase() { return 100; }
}
