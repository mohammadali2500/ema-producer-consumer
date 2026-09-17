package com.example.fx.consumer.health;

import com.example.fx.consumer.config.ConsumerControlProperties;
import com.example.fx.consumer.control.KafkaConsumerLifecycle;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("fxKafkaConsumer")
public class KafkaConsumerHealthIndicator implements HealthIndicator {
    private final KafkaConsumerLifecycle lifecycle;
    private final ConsumerControlProperties properties;

    public KafkaConsumerHealthIndicator(
            KafkaConsumerLifecycle lifecycle,
            ConsumerControlProperties properties) {
        this.lifecycle = lifecycle;
        this.properties = properties;
    }

    @Override
    public Health health() {
        // A deliberately stopped standby remains a healthy pod.
        return Health.up()
                .withDetail("zone", properties.zone())
                .withDetail("vaultAutoStart", properties.autoStart())
                .withDetail("listenerRunning", lifecycle.isRunning())
                .withDetail("role", lifecycle.isRunning() ? "ACTIVE" : "STANDBY")
                .build();
    }
}
