package com.example.fx.producer.ema;

import com.refinitiv.ema.access.EmaFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EmaFactoryConfiguration {
    @Bean
    EmaConsumerFactory emaConsumerFactory() {
        return EmaFactory::createOmmConsumer;
    }
}
