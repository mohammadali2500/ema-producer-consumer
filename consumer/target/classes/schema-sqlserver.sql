CREATE TABLE fx_quote_event (
    event_id varchar(36) NOT NULL,
    instrument varchar(128) NOT NULL,
    received_at datetime2(7) NOT NULL,
    source_zone varchar(64) NOT NULL,
    payload varchar(max) NOT NULL,
    CONSTRAINT pk_fx_quote_event PRIMARY KEY (event_id)
);

CREATE INDEX ix_fx_quote_event_instrument_received_at
    ON fx_quote_event (instrument, received_at DESC);
