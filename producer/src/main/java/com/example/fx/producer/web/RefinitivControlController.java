package com.example.fx.producer.web;

import com.example.fx.producer.config.RefinitivProperties;
import com.example.fx.producer.control.RefinitivLifecycle;
import com.example.fx.producer.ema.EmaConnectionStatus;
import com.example.fx.producer.kafka.QuotePublisher;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/operations/refinitiv")
public class RefinitivControlController {
    private final RefinitivLifecycle lifecycle;
    private final RefinitivProperties properties;
    private final EmaConnectionStatus connectionStatus;
    private final QuotePublisher quotePublisher;

    public RefinitivControlController(
            RefinitivLifecycle lifecycle,
            RefinitivProperties properties,
            EmaConnectionStatus connectionStatus,
            QuotePublisher quotePublisher) {
        this.lifecycle = lifecycle;
        this.properties = properties;
        this.connectionStatus = connectionStatus;
        this.quotePublisher = quotePublisher;
    }

    @PostMapping("/start")
    public ResponseEntity<Map<String, Object>> start() {
        lifecycle.start();
        return ResponseEntity.ok(status());
    }

    @PostMapping("/stop")
    public ResponseEntity<Map<String, Object>> stop() {
        lifecycle.stop();
        return ResponseEntity.ok(status());
    }

    @GetMapping
    public Map<String, Object> status() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("zone", properties.zone());
        result.put("vaultAutoStart", properties.autoStart());
        result.put("lifecycleRunning", lifecycle.isRunning());
        result.put("connectionState", connectionStatus.state());
        result.put("statusText", connectionStatus.statusText());
        result.put("lastMessageAt", connectionStatus.lastMessageAt());
        result.put("lastQuoteAt", connectionStatus.lastQuoteAt());
        result.put("lastKafkaSuccessAt", quotePublisher.lastSuccessfulSendAt());
        return result;
    }
}
