# FX pipeline lifecycle reference

This project demonstrates the agreed lifecycle model:

- `producer`: consumes LSEG/Refinitiv EMA data and publishes quotes to Kafka.
- `consumer`: consumes Kafka records and persists them with JPA.
- `common`: contains the shared Kafka event contract.

Both applications read a zone-specific auto-start flag injected from Vault when the pod starts. A manual start/stop is deliberately local to the current pod. After a crash or restart, the Vault-injected value is applied again.

## Vault contract

The example is intentionally independent of a particular Vault injector. Supply the Vault values as environment variables using your organisation's Vault Agent, CSI driver, External Secrets operator, or Spring Cloud Vault setup.

Producer zone:

```text
REFINITIV_AUTO_START=true|false
REFINITIV_HOST=...
REFINITIV_PORT=14002
REFINITIV_USERNAME=...
REFINITIV_PASSWORD=...
REFINITIV_SERVICE_NAME=...
REFINITIV_INSTRUMENTS=EUR=,GBP=
ZONE_NAME=zone-1
KAFKA_BOOTSTRAP_SERVERS=...
FX_QUOTES_TOPIC=fx-quotes
```

Consumer zone:

```text
KAFKA_CONSUMER_AUTO_START=true|false
ZONE_NAME=zone-1
KAFKA_BOOTSTRAP_SERVERS=...
KAFKA_CONSUMER_GROUP=fx-database-consumer
FX_QUOTES_TOPIC=fx-quotes
DATABASE_URL=jdbc:sqlserver://host:1433;databaseName=fx
DATABASE_USERNAME=...
DATABASE_PASSWORD=...
```

Do not expose Vault secrets through Actuator or the status endpoints.

## Runtime behaviour

Producer:

```text
Vault auto-start=true  -> SmartLifecycle.start() creates OmmConsumer
Manual stop            -> OmmConsumer.uninitialize()
Manual start           -> new OmmConsumer + subscriptions
Network disconnect     -> EMA reconnects; SmartLifecycle does nothing
Pod restart            -> Vault auto-start is applied again
```

Consumer:

```text
Vault auto-start=true  -> listener container starts after ApplicationReadyEvent
Vault auto-start=false -> pod remains healthy; listener stays outside the group
Manual stop            -> listener container stop()
Manual start           -> listener container start()
Pod restart            -> Vault auto-start is applied again
```

Do not use `pause()` for a standby consumer because it can remain in the consumer group and retain partition assignments. Use `stop()`.

## Operational endpoints

The sample exposes:

```text
GET  /operations/refinitiv
POST /operations/refinitiv/start
POST /operations/refinitiv/stop

GET  /operations/kafka-consumer
POST /operations/kafka-consumer/start
POST /operations/kafka-consumer/stop
```

Protect these endpoints with your organisation's SSO/service-mesh authorization or Spring Security before deployment. They are intentionally not given a generic authentication scheme because production identity integration is organisation-specific.

## Important production adaptations

1. Replace the sample EMA `message.toString()` mapping with your field-ID-to-domain mapping.
2. Replace the sample UUID calculation with the stable event identity supplied by your feed/business contract.
3. Apply the SQL migration for `fx_quote_event`; the sample uses `ddl-auto=validate`.
4. Use a unique constraint/upsert appropriate for SQL Server. `existsById` is illustrative and is not sufficient protection against every concurrent duplicate race.
5. Configure EMA reconnect settings in your existing `EmaConfig.xml` or programmatic EMA configuration. EMA defaults to unlimited reconnect attempts; do not add Quartz reconnect jobs.
6. Keep liveness independent of upstream EMA availability. Use readiness/metrics for connection and feed state.
7. Track Kafka send failures and decide when a prolonged Kafka outage should stop EMA intake.
8. Set OpenShift `terminationGracePeriodSeconds` long enough for EMA shutdown and Kafka/database work to finish.

## Build

```bash
./mvnw clean verify
```

The repository does not include a generated Maven wrapper. Either add your organisation's approved wrapper or use Maven 3.9+:

```bash
mvn clean verify
```

## Version note

The sample pins EMA Java `3.10.0.2`, matching the 3.10 configuration guide generation. Align this with the RTSDK version approved by your organisation.
