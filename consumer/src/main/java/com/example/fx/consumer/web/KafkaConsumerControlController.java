package com.example.fx.consumer.web;

import com.example.fx.consumer.config.ConsumerControlProperties;
import com.example.fx.consumer.control.KafkaConsumerLifecycle;
import com.example.fx.consumer.kafka.FxQuoteListener;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/operations/kafka-consumer")
public class KafkaConsumerControlController {
    private final KafkaConsumerLifecycle lifecycle;
    private final ConsumerControlProperties properties;
    private final FxQuoteListener listener;

    public KafkaConsumerControlController(
            KafkaConsumerLifecycle lifecycle,
            ConsumerControlProperties properties,
            FxQuoteListener listener) {
        this.lifecycle = lifecycle;
        this.properties = properties;
        this.listener = listener;
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
        result.put("listenerRunning", lifecycle.isRunning());
        result.put("lastProcessedAt", listener.lastProcessedAt());
        return result;
    }
}
