package com.example.fx.common;

import java.time.Instant;

public record FxQuoteEvent(
        String eventId,
        String instrument,
        Instant receivedAt,
        String sourceZone,
        String payload) {}
