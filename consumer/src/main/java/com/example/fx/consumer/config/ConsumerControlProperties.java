package com.example.fx.consumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("app.consumer")
public record ConsumerControlProperties(
        boolean autoStart,
        String listenerId,
        String zone) {}
