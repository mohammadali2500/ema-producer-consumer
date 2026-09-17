package com.example.fx.producer.ema;

import com.example.fx.common.FxQuoteEvent;
import com.example.fx.producer.config.RefinitivProperties;
import com.example.fx.producer.kafka.QuotePublisher;
import com.refinitiv.ema.access.AckMsg;
import com.refinitiv.ema.access.EmaFactory;
import com.refinitiv.ema.access.GenericMsg;
import com.refinitiv.ema.access.OmmConsumer;
import com.refinitiv.ema.access.OmmConsumerClient;
import com.refinitiv.ema.access.OmmConsumerEvent;
import com.refinitiv.ema.access.OmmState;
import com.refinitiv.ema.access.RefreshMsg;
import com.refinitiv.ema.access.ReqMsg;
import com.refinitiv.ema.access.StatusMsg;
import com.refinitiv.ema.access.UpdateMsg;
import com.refinitiv.ema.rdm.EmaRdm;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class RefinitivQuoteClient implements OmmConsumerClient {
    private static final Logger log = LoggerFactory.getLogger(RefinitivQuoteClient.class);

    private final RefinitivProperties properties;
    private final QuotePublisher publisher;
    private final EmaConnectionStatus connectionStatus;

    public RefinitivQuoteClient(
            RefinitivProperties properties,
            QuotePublisher publisher,
            EmaConnectionStatus connectionStatus) {
        this.properties = properties;
        this.publisher = publisher;
        this.connectionStatus = connectionStatus;
    }

    public void registerSubscriptions(OmmConsumer consumer) {
        for (String instrument : properties.instruments()) {
            ReqMsg request = EmaFactory.createReqMsg()
                    .serviceName(properties.serviceName())
                    .name(instrument)
                    .interestAfterRefresh(true);
            consumer.registerClient(request, this);
        }
    }

    @Override
    public void onRefreshMsg(RefreshMsg message, OmmConsumerEvent event) {
        connectionStatus.messageReceived();
        updateState(message.state(), message.domainType());
        if (message.domainType() == EmaRdm.MMT_MARKET_PRICE) {
            publish(message.hasName() ? message.name() : "UNKNOWN", message.toString());
        }
    }

    @Override
    public void onUpdateMsg(UpdateMsg message, OmmConsumerEvent event) {
        connectionStatus.quoteReceived();
        publish(message.hasName() ? message.name() : "UNKNOWN", message.toString());
    }

    @Override
    public void onStatusMsg(StatusMsg message, OmmConsumerEvent event) {
        connectionStatus.messageReceived();
        updateState(message.hasState() ? message.state() : null, message.domainType());
    }

    private void publish(String instrument, String payload) {
        Instant receivedAt = Instant.now();
        String identity = properties.zone() + '|' + instrument + '|' + receivedAt + '|' + payload;
        String eventId = UUID.nameUUIDFromBytes(identity.getBytes(StandardCharsets.UTF_8)).toString();
        publisher.publish(new FxQuoteEvent(
                eventId, instrument, receivedAt, properties.zone(), payload));
    }

    private void updateState(OmmState state, int domainType) {
        if (state == null) {
            return;
        }
        boolean openAndOk = state.streamState() == OmmState.StreamState.OPEN
                && state.dataState() == OmmState.DataState.OK;
        if (openAndOk && domainType == EmaRdm.MMT_LOGIN) {
            connectionStatus.update(EmaConnectionStatus.State.CONNECTED, state.statusText());
        } else if (!openAndOk) {
            connectionStatus.update(EmaConnectionStatus.State.RECONNECTING, state.statusText());
        }
        log.info("EMA state domain={} stream={} data={} text={}",
                domainType, state.streamState(), state.dataState(), state.statusText());
    }

    @Override public void onGenericMsg(GenericMsg message, OmmConsumerEvent event) {}
    @Override public void onAckMsg(AckMsg message, OmmConsumerEvent event) {}
    @Override public void onAllMsg(com.refinitiv.ema.access.Msg message, OmmConsumerEvent event) {}
}
