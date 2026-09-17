package com.example.fx.consumer.kafka;

import com.example.fx.common.FxQuoteEvent;
import com.example.fx.consumer.persistence.FxQuoteEntity;
import com.example.fx.consumer.persistence.FxQuoteRepository;
import jakarta.transaction.Transactional;
import java.time.Instant;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class FxQuoteListener {
    private final FxQuoteRepository repository;
    private volatile Instant lastProcessedAt;

    public FxQuoteListener(FxQuoteRepository repository) {
        this.repository = repository;
    }

    @Transactional
    @KafkaListener(
            id = "${app.consumer.listener-id}",
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            autoStartup = "false")
    public void consume(FxQuoteEvent event) {
        if (!repository.existsById(event.eventId())) {
            repository.save(new FxQuoteEntity(event));
        }
        lastProcessedAt = Instant.now();
    }

    public Instant lastProcessedAt() {
        return lastProcessedAt;
    }
}
