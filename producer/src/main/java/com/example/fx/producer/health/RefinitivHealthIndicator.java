package com.example.fx.producer.health;

import com.example.fx.producer.control.RefinitivLifecycle;
import com.example.fx.producer.ema.EmaConnectionStatus;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component("refinitiv")
public class RefinitivHealthIndicator implements HealthIndicator {
    private final RefinitivLifecycle lifecycle;
    private final EmaConnectionStatus status;

    public RefinitivHealthIndicator(RefinitivLifecycle lifecycle, EmaConnectionStatus status) {
        this.lifecycle = lifecycle;
        this.status = status;
    }

    @Override
    public Health health() {
        Health.Builder builder = switch (status.state()) {
            case CONNECTED -> Health.up();
            case STOPPED -> Health.unknown();
            case CONNECTING, RECONNECTING -> Health.outOfService();
            case FAILED -> Health.down();
        };
        return builder
                .withDetail("lifecycleRunning", lifecycle.isRunning())
                .withDetail("connectionState", status.state())
                .withDetail("statusText", status.statusText())
                .withDetail("lastQuoteAt", status.lastQuoteAt())
                .build();
    }
}
