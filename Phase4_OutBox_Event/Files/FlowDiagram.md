# Order–Inventory–Payment Saga — Event Flow Documentation

Base: `saga-orchestrator-service` (Kafka-based orchestration), coordinating
`order-service`, `inventory-service`, and `payment-service`. Every step below
is verified against real logs from this project (see log for orderId 5, 6, 7).

---

## 1. Services & Topics

| Service | Role |
|---|---|
| `order-service` | Owns Order data. Persists PENDING order, publishes `OrderCreated`. Reacts to `order.confirm.command` / `order.failed.command` to set final status. |
| `saga-orchestrator-service` | Owns `SagaState`. Listens to every reply event, decides the next command, and drives the saga to a terminal state (COMPLETED or FAILED). |
| `inventory-service` | Owns Product/stock data. Reacts to reserve/release commands, replies with the outcome. |
| `payment-service` | Owns Payment data. Reacts to process command, replies with the outcome. |

All services publish through the **outbox pattern**: a business change and an
`OutboxEvent` row are saved in the same local transaction, then a
`@Scheduled` `OutboxPoller` reads pending rows and sends them to Kafka as raw
JSON strings (`KafkaTemplate<String, String>`), separate from any
`KafkaTemplate<String, Object>` used for direct object sends.

---

## 2. Full Event/Command Table

| # | Topic | Type | Producer | Consumer | Payload (key fields) |
|---|---|---|---|---|---|
| 1 | `order-created` | Event | order-service | saga-orchestrator-service | orderId, items[] |
| 2 | `inventory.reserve.command` | Command | saga-orchestrator-service | inventory-service | orderId, items[] |
| 3 | `inventory.reserved.event` | Event | inventory-service | saga-orchestrator-service | orderId, totalAmount |
| 4 | `inventory.rejected.event` | Event | inventory-service | saga-orchestrator-service | orderId, reason |
| 5 | `payment.process.command` | Command | saga-orchestrator-service | payment-service | orderId, amount |
| 6 | `payment.completed.event` | Event | payment-service | saga-orchestrator-service | orderId |
| 7 | `payment.failed.event` | Event | payment-service | saga-orchestrator-service | orderId, reason |
| 8 | `order.confirm.command` | Command | saga-orchestrator-service | order-service | orderId |
| 9 | `inventory.release.command` | Command | saga-orchestrator-service | inventory-service | orderId, items[] |
| 10 | `inventory.released.event` | Event | inventory-service | saga-orchestrator-service | orderId |
| 11 | `order.failed.command` | Command | saga-orchestrator-service | order-service | orderId, reason |

*(Topic names 3/4/6/7/10 shown as full event names for clarity — confirm exact
`KafkaTopics` constant strings against your `KafkaTopics.java` per service if
they differ slightly, e.g. `INVENTORY_RESERVED` vs `inventory.reserved.event`.)*

---

## 3. Path A — Happy Path (verified: orderId 5)

```
order-service          : place order -> save PENDING -> publish OrderCreated
saga-orchestrator       : consume OrderCreated -> SagaState STARTED
                         -> publish ReserveInventoryCommand  
                         
inventory-service       : consume command -> stock sufficient -> deduct stock
                         -> publish InventoryReserved  
                         
saga-orchestrator       : consume InventoryReserved -> SagaState INVENTORY_RESERVED
                         -> publish ProcessPaymentCommand  
                         
payment-service         : consume command -> charge succeeds
                         -> publish PaymentCompleted  
                         
saga-orchestrator       : consume PaymentCompleted -> SagaState PAYMENT_COMPLETED
                         -> publish ConfirmOrderCommand  
                         
order-service           : consume command -> set order COMPLETED
saga-orchestrator       : SagaState -> COMPLETED
```

## 4. Path B — Inventory Rejected (verified: orderId 6)

```
order-service          : place order -> save PENDING -> publish OrderCreated  

saga-orchestrator       : consume OrderCreated -> SagaState STARTED
                         -> publish ReserveInventoryCommand  
                         
inventory-service       : consume command -> insufficient stock
                         -> publish InventoryRejected (no stock change)  
                         
saga-orchestrator       : consume InventoryRejected -> SagaState FAILED
                         -> publish OrderFailedCommand  
                         
order-service           : consume command -> set order FAILED
```

Payment is never contacted in this path — the saga aborts at the first step.

## 5. Path C — Payment Failed / Compensation (verified: orderId 7)

```
order-service          : place order -> save PENDING -> publish OrderCreated  

saga-orchestrator       : consume OrderCreated -> SagaState STARTED
                         -> publish ReserveInventoryCommand  
                         
inventory-service       : consume command -> stock sufficient -> deduct stock
                         -> publish InventoryReserved  
                         
saga-orchestrator       : consume InventoryReserved -> SagaState INVENTORY_RESERVED
                         -> publish ProcessPaymentCommand  
                         
payment-service         : consume command -> charge declined
                         -> publish PaymentFailed  
                         
saga-orchestrator       : consume PaymentFailed -> SagaState COMPENSATING
                         -> publish ReleaseInventoryCommand  
                         
inventory-service       : consume command -> restore stock
                         -> publish InventoryReleased  
                         
saga-orchestrator       : consume InventoryReleased -> SagaState FAILED (compensated)
                         -> publish OrderFailedCommand  
                         
order-service           : consume command -> set order FAILED
```

This is the exact scenario that stayed permanently broken in Phase 2 (stock
deducted, never restored, order stuck). Here it self-heals.

---

## 6. Delivery Mechanics (Outbox Pattern)

Every arrow labeled "via outbox" above follows this sequence, not a direct
`kafkaTemplate.send()`:

```
1. Listener/service handles a command inside @Transactional method
2. Business row updated (e.g. Product.quantity, Order.status)
3. OutboxEvent row saved in the SAME transaction (topic, JSON payload string)
4. Both commit together, or neither does — no "saved but never published" gap
5. @Scheduled OutboxPoller (separate thread, fixedDelay) reads PENDING rows
6. Poller sends event.getPayload() as-is via KafkaTemplate<String, String>
   (raw pass-through — NOT re-serialized as an object, avoids double-encoding)
7. Row marked published
```

Why two `KafkaTemplate` beans exist per service:
- `KafkaTemplate<String, Object>` (JacksonJsonSerializer) — for any code path
  that sends a live Java object directly (e.g. `order-service`'s
  `OrderCreated` publish in `placeOrder()`).
- `KafkaTemplate<String, String>` (StringSerializer) — used only by
  `OutboxPoller`, because the payload is already a finished JSON string;
  running it through a JSON serializer again would wrap it in an extra layer
  of quotes and break consumer-side deserialization.

---

## 7. Idempotency Guard

Each reactive listener (inventory reserve/release, payment process, order
confirm/failed) checks a `processed_events` (or equivalent) table keyed by
`orderId` + event type **before** applying any effect:

```
if already processed(orderId, EVENT_TYPE):
    log + return   # no-op, safe under Kafka at-least-once redelivery
else:
    apply effect
    record processed(orderId, EVENT_TYPE)
```

This has been implemented for inventory's reserve/release listeners
(confirmed in code). **Not yet confirmed by an actual duplicate-delivery
test** — recommended next step per the roadmap (Step 7.3).

---

## 9. Open / Not Yet Verified

- Idempotency: force a real duplicate redelivery per listener and confirm no-op.
- Downstream DB confirmation: verify `order-service`'s `orders` table and
  `inventory-service`'s `product` table actually reflect the final state for
  orderId 5, 6, 7 (log shows commands published, not DB row values).
- Repeated runs of all three paths (timing/ordering robustness).
- Outbox crash-safety test: kill the app after a DB commit but before the
  poller runs, restart, confirm the pending event still gets published.