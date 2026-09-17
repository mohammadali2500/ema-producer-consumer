package com.example.fx.producer.ema;

import java.time.Instant;
import org.springframework.stereotype.Component;

@Component
public class EmaConnectionStatus {
    public enum State { STOPPED, CONNECTING, CONNECTED, RECONNECTING, FAILED }

    private volatile State state = State.STOPPED;
    private volatile String statusText = "Not started";
    private volatile Instant lastMessageAt;
    private volatile Instant lastQuoteAt;

    public void update(State state, String statusText) {
        this.state = state;
        this.statusText = statusText;
    }

    public void messageReceived() {
        lastMessageAt = Instant.now();
    }

    public void quoteReceived() {
        Instant now = Instant.now();
        lastMessageAt = now;
        lastQuoteAt = now;
    }

    public State state() { return state; }
    public String statusText() { return statusText; }
    public Instant lastMessageAt() { return lastMessageAt; }
    public Instant lastQuoteAt() { return lastQuoteAt; }
}
