package com.example.fx.producer.kafka;

import com.example.fx.common.FxQuoteEvent;
import com.example.fx.producer.config.RefinitivProperties;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class QuotePublisher {
    private static final Logger log = LoggerFactory.getLogger(QuotePublisher.class);

    private final KafkaTemplate<String, FxQuoteEvent> kafkaTemplate;
    private final RefinitivProperties properties;
    private volatile Instant lastSuccessfulSendAt;

    public QuotePublisher(
            KafkaTemplate<String, FxQuoteEvent> kafkaTemplate,
            RefinitivProperties properties) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    public void publish(FxQuoteEvent quote) {
        kafkaTemplate.send(properties.kafkaTopic(), quote.instrument(), quote)
                .whenComplete((result, failure) -> {
                    if (failure == null) {
                        lastSuccessfulSendAt = Instant.now();
                    } else {
                        log.error("Kafka publish failed for event {}", quote.eventId(), failure);
                    }
                });
    }

    public void flush() {
        kafkaTemplate.flush();
    }

    public Instant lastSuccessfulSendAt() {
        return lastSuccessfulSendAt;
    }
}
