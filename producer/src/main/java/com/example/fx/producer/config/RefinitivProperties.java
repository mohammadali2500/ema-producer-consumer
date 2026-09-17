package com.example.fx.producer.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("app.refinitiv")
public record RefinitivProperties(
        boolean autoStart,
        @NotBlank String host,
        @Positive int port,
        @NotBlank String username,
        String password,
        @NotBlank String serviceName,
        @NotEmpty List<String> instruments,
        @NotBlank String zone,
        @NotBlank String kafkaTopic) {}
