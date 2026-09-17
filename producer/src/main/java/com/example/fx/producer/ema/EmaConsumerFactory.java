package com.example.fx.producer.ema;

import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerClient;
import com.refinitiv.ema.access.OmmConsumerConfig;

@FunctionalInterface
public interface EmaConsumerFactory {
    OmmConsumer create(OmmConsumerConfig configuration, OmmConsumerClient administrativeClient);
}
