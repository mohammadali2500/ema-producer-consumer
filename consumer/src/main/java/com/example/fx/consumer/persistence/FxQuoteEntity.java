package com.example.fx.consumer.persistence;

import com.example.fx.common.FxQuoteEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "fx_quote_event")
public class FxQuoteEntity {
    @Id
    @Column(name = "event_id", length = 36, nullable = false)
    private String eventId;

    @Column(name = "instrument", length = 128, nullable = false)
    private String instrument;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "source_zone", length = 64, nullable = false)
    private String sourceZone;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    protected FxQuoteEntity() {}

    public FxQuoteEntity(FxQuoteEvent event) {
        this.eventId = event.eventId();
        this.instrument = event.instrument();
        this.receivedAt = event.receivedAt();
        this.sourceZone = event.sourceZone();
        this.payload = event.payload();
    }

    public String getEventId() { return eventId; }
}
