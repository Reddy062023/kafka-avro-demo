# Kafka + RabbitMQ + G4S Pipeline — Complete Reference Guide

> A production-grade event-driven pipeline built on top of kafka-avro-demo.
> Sales transactions flow through Kafka, CASH transactions are routed via RabbitMQ
> to the G4S Manager Service which marks them as SAFE.

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Java 17, Spring Boot |
| Event Streaming | Apache Kafka + Avro |
| Message Broker | RabbitMQ |
| Database | MongoDB |
| API Testing | Karate Framework |
| Manual Testing | Postman |
| Containerization | Docker |

---

## Table of Contents

1. [What We Built — The Big Picture](#1-what-we-built--the-big-picture)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Why Kafka AND RabbitMQ?](#3-why-kafka-and-rabbitmq)
4. [Content-Based Routing Explained](#4-content-based-routing-explained)
5. [New Files Added](#5-new-files-added)
6. [RabbitMQ Setup](#6-rabbitmq-setup)
7. [Running the Full Pipeline](#7-running-the-full-pipeline)
8. [Testing with Postman](#8-testing-with-postman)
9. [RabbitMQ Management UI Guide](#9-rabbitmq-management-ui-guide)
10. [Manual Test Cases](#10-manual-test-cases)
11. [MongoDB Validation Queries](#11-mongodb-validation-queries)
12. [Karate Automation](#12-karate-automation)
13. [QA Testing Coverage](#13-qa-testing-coverage)
14. [Common Errors and Fixes](#14-common-errors-and-fixes)
15. [Key Learnings](#15-key-learnings)
16. [Interview Q&A](#16-interview-qa)
17. [Quick Reference Commands](#17-quick-reference-commands)

---

## 1. What We Built — The Big Picture

### Business Scenario

Kroger POSaaS (Point of Sale as a Service) generates thousands of sales
transactions per minute from every store location. These include:

- CASH payments
- CARD payments (Credit / Debit)
- GIFT_CERT (Gift Certificates)
- CHECK payments
- STORE_CARD payments

**The problem:** G4S (a physical cash security company) needs to be notified
ONLY about CASH transactions so they can confirm the cash has been secured
in the physical safe and mark it as **SAFE**.

**The solution:** A content-based routing pipeline:
1. ALL transactions go into Kafka
2. A Filter Service reads from Kafka and routes CASH → RabbitMQ
3. RabbitMQ delivers to the G4S Manager Service
4. G4S Manager updates the transaction status to **SAFE**

### Payment Routing Rules

| Payment Type | Goes to Kafka | Goes to RabbitMQ | G4S Updates | Final Status |
|-------------|--------------|-----------------|------------|-------------|
| CASH | YES | YES — cash.queue | YES | **SAFE** |
| CARD | YES | NO — filtered | NO | PROCESSED |
| GIFT_CERT | YES | NO — filtered | NO | PROCESSED |
| CHECK | YES | NO — filtered | NO | PROCESSED |
| STORE_CARD | YES | NO — filtered | NO | PROCESSED |

---

## 2. Architecture Diagram

```
  Postman / Ingress Microservice
          │
          │  POST /test/kafka/publish/sales-transactions
          │  { transactionId, type: CASH|CARD|GIFT, amount }
          ▼
  ┌──────────────────────────┐
  │  Kafka Topic:            │  ← ALL transactions land here
  │  sales-transactions      │     (CASH + CARD + GIFT + CHECK)
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │  Filter Service          │  ← SalesTransactionConsumer.java
  │  (filter-group consumer) │     Reads JSON from Kafka
  │                          │     Business rule: type == CASH?
  └────┬─────────────┬───────┘
       │             │
    CASH?         NON-CASH?
       │             │
       ▼             ▼
  ┌─────────┐   ┌──────────────────┐
  │RabbitMQ │   │  status=PROCESSED │
  │cash.    │   │  saved to MongoDB │
  │queue    │   └──────────────────┘
  └────┬────┘
       │
       ▼
  ┌──────────────────────────┐
  │  G4S Manager Service     │  ← G4SManagerService.java
  │  (RabbitMQ consumer)     │     Reads from cash.queue
  │                          │     Updates status = SAFE
  └──────────┬───────────────┘
             │
             ▼
  ┌──────────────────────────┐
  │  MongoDB                 │  ← Final state: status = SAFE
  │  kafkadb.transactions    │
  └──────────────────────────┘
```

### Full Project Architecture (Combined)

```
  Postman
     │
     ├── POST /orders              → Kafka (Avro) → OrderConsumer
     │                               → MongoDB (orders collection)
     │                               → PostgreSQL (audit log)
     │
     └── POST /test/kafka/publish  → Kafka (JSON) → FilterConsumer
                                     → CASH only → RabbitMQ
                                     → G4S Service
                                     → MongoDB (transactions collection)
```

---

## 3. Why Kafka AND RabbitMQ?

They serve completely different purposes and complement each other perfectly.

| Feature | Kafka | RabbitMQ |
|---------|-------|----------|
| Type | Distributed log | Message broker |
| Messages kept after read | YES — forever | NO — gone after consumed |
| Best for | High-volume ingestion, audit, replay | Task queues, targeted routing |
| Message ordering | Per partition | Per queue |
| Consumer model | Pull (consumer reads at own pace) | Push (broker delivers to consumer) |
| Replay old messages | YES | NO |
| Dead Letter Queue | YES (separate topic) | YES (built-in) |
| Management UI | Offset Explorer | RabbitMQ UI (port 15672) |
| Used for in this project | ALL transactions ingestion | CASH-only routing to G4S |

### Why not put everything in Kafka?

We could have a separate Kafka topic `cash-transactions` but:
- RabbitMQ is better for task-based workflows like G4S processing
- RabbitMQ DLQ is simpler to configure for retry logic
- G4S system expects RabbitMQ (common in enterprise integrations)
- Keeps Kafka as the audit layer and RabbitMQ as the workflow layer

---

## 4. Content-Based Routing Explained

**Content-based routing** means: look inside the message content and
decide where to send it based on a business rule.

```java
// SalesTransactionConsumer.java — the routing logic
if ("CASH".equalsIgnoreCase(txn.getType())) {
    // Route to RabbitMQ for G4S processing
    rabbitTemplate.convertAndSend("cash.exchange", "cash.routing.key", txn);
} else {
    // Skip G4S — just mark as PROCESSED
    txn.setStatus("PROCESSED");
    repository.save(txn);
}
```

This is different from **topic-based routing** where the producer
decides which topic to write to. In content-based routing:
- Producer writes everything to ONE topic
- Consumer reads and decides where to route based on content
- Keeps the producer simple and unaware of downstream systems

---

## 5. New Files Added

```
src/main/java/com/demo/
│
├── transaction/
│   ├── SalesTransaction.java          MongoDB document model
│   ├── TransactionRepository.java     MongoDB repository
│   ├── TransactionController.java     HTTP bridge endpoints
│   └── SalesTransactionConsumer.java  Kafka consumer + CASH filter
│
├── g4s/
│   ├── G4SManagerService.java         RabbitMQ consumer → updates to SAFE
│   └── G4SController.java             REST endpoints for verification
│
└── config/
    ├── RabbitMQConfig.java             Queue, exchange, binding, DLQ setup
    └── KafkaStringConfig.java          String Kafka template + consumer factory
```

### New API Endpoints

| Method | URL | Purpose |
|--------|-----|---------|
| POST | `/test/kafka/publish/{topic}` | HTTP bridge — publish to Kafka |
| GET | `/test/kafka/transactions/{id}` | Get transaction by ID |
| GET | `/test/kafka/transactions` | Get all transactions |
| DELETE | `/test/kafka/cleanup` | Delete test data |
| GET | `/g4s/transactions/{id}` | Check if transaction is SAFE |
| GET | `/g4s/transactions` | Get all SAFE transactions |
| GET | `/g4s/summary` | Count by status |

### New Dependencies Added to `pom.xml`

```xml
<!-- RabbitMQ -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-amqp</artifactId>
</dependency>

<!-- Jackson Java 8 Date/Time support -->
<dependency>
    <groupId>com.fasterxml.jackson.datatype</groupId>
    <artifactId>jackson-datatype-jsr310</artifactId>
</dependency>

<!-- JSON ObjectMapper -->
<dependency>
    <groupId>com.fasterxml.jackson.core</groupId>
    <artifactId>jackson-databind</artifactId>
</dependency>
```

### New `application.yml` additions

```yaml
spring:
  rabbitmq:
    host: localhost
    port: 5672
    username: guest
    password: guest
    virtual-host: /
```

---

## 6. RabbitMQ Setup

### RabbitMQ Container (already running)

```
Container: rabbitmq
Port 5672:  AMQP protocol (app connects here)
Port 15672: Management UI (browser connects here)
```

### Verify RabbitMQ is running

```cmd
docker ps | findstr rabbitmq
```

### RabbitMQ Management UI

```
URL:      http://localhost:15672
Username: guest
Password: guest
```

### Queues Created Automatically on First Message

| Queue | Purpose | Features |
|-------|---------|---------|
| `cash.queue` | Receives CASH transactions from Filter Service | Durable (D), Dead Letter Exchange (DLX) |
| `cash.dlq` | Receives failed messages from cash.queue | Durable (D) |

### Exchange Created

| Exchange | Type | Purpose |
|----------|------|---------|
| `cash.exchange` | Direct | Routes messages using routing key |
| `cash.dlq.exchange` | Direct | Routes failed messages to DLQ |

### Binding

```
cash.exchange  →  routing key: cash.routing.key  →  cash.queue
cash.dlq.exchange  →  routing key: cash.queue  →  cash.dlq
```

### RabbitMQ UI Tabs Explained

| Tab | What to look for |
|-----|-----------------|
| Overview | Connection count, message rates graph |
| Connections | Active Spring Boot connections |
| Channels | Active message channels |
| Exchanges | cash.exchange and cash.dlq.exchange |
| Queues | cash.queue and cash.dlq message counts |

---

## 7. Running the Full Pipeline

### Prerequisites

```cmd
# Verify all containers are running
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

Expected containers:
```
kafka            Up    0.0.0.0:9092->9092/tcp
rabbitmq         Up    0.0.0.0:5672->5672/tcp, 0.0.0.0:15672->15672/tcp
mongodb          Up    0.0.0.0:27017->27017/tcp
qa-postgres      Up    0.0.0.0:5432->5432/tcp
schema-registry  Up    0.0.0.0:8081->8081/tcp
zookeeper        Up    0.0.0.0:2181->2181/tcp
```

### Start the App

```cmd
cd C:\kafka-avro-demo
mvn spring-boot:run
```

### Verify All 3 Consumer Groups Started

Look for these 3 lines in the startup output:

```
demo-group:   partitions assigned: [orders-0]
dlq-group:    partitions assigned: [orders-dlq-0]
filter-group: partitions assigned: [sales-transactions-0]
```

### Verify App is Running

```cmd
curl http://localhost:8085/orders -UseBasicParsing
```

---

## 8. Testing with Postman

### IMPORTANT: Postman URL Tip

Always **type URLs manually** in Postman — do not copy-paste.
Copying can add invisible characters like `%0A` (newline) that break requests.

### Collection Structure

Create a Postman collection: `kafka-rabbitmq-g4s-pipeline`

```
├── 1_publish
│   ├── Publish CASH transaction
│   ├── Publish CARD transaction
│   ├── Publish GIFT_CERT transaction
│   └── Publish invalid (no type)
├── 2_verify_g4s
│   ├── GET CASH transaction (expect SAFE)
│   ├── GET CARD transaction (expect PROCESSED)
│   └── GET G4S summary
└── 3_cleanup
    └── DELETE cleanup test data
```

### Request 1: Publish CASH Transaction

```
Method:  POST
URL:     http://localhost:8085/test/kafka/publish/sales-transactions
Headers: Content-Type: application/json

Body:
{
    "transactionId": "POSTMAN-CASH-001",
    "type":          "CASH",
    "amount":        500.00,
    "currency":      "USD",
    "storeId":       "STORE-001"
}
```

Expected Response (200 OK):
```json
{
    "message":       "Published to Kafka topic: sales-transactions",
    "transactionId": "POSTMAN-CASH-001",
    "type":          "CASH",
    "amount":        500.0,
    "kafkaTopic":    "sales-transactions",
    "status":        "CREATED"
}
```

Postman Tests tab:
```javascript
pm.test("Status 200", () => pm.response.to.have.status(200));
pm.test("Published to Kafka", () => {
    pm.expect(pm.response.json().kafkaTopic)
      .to.eql("sales-transactions");
});
pm.test("Type is CASH", () => {
    pm.expect(pm.response.json().type).to.eql("CASH");
});
pm.environment.set("lastCashId", pm.response.json().transactionId);
```

### Request 2: Publish CARD Transaction (should NOT become SAFE)

```json
{
    "transactionId": "POSTMAN-CARD-001",
    "type":          "CARD",
    "amount":        350.00,
    "currency":      "USD",
    "storeId":       "STORE-001"
}
```

### Request 3: Verify CASH is SAFE (wait 5 seconds first)

```
Method: GET
URL:    http://localhost:8085/g4s/transactions/POSTMAN-CASH-001
```

Expected (200 OK):
```json
{
    "transactionId": "POSTMAN-CASH-001",
    "type":          "CASH",
    "amount":        500.0,
    "status":        "SAFE"
}
```

Postman Tests tab:
```javascript
pm.test("Status 200", () => pm.response.to.have.status(200));
pm.test("Status is SAFE", () => {
    pm.expect(pm.response.json().status).to.eql("SAFE");
});
pm.test("Type is CASH", () => {
    pm.expect(pm.response.json().type).to.eql("CASH");
});
```

### Request 4: Verify CARD is NOT SAFE

```
Method: GET
URL:    http://localhost:8085/g4s/transactions/POSTMAN-CARD-001
```

Expected:
```json
{
    "transactionId": "POSTMAN-CARD-001",
    "type":          "CARD",
    "status":        "PROCESSED"
}
```

Postman Tests tab:
```javascript
pm.test("Status is PROCESSED not SAFE", () => {
    pm.expect(pm.response.json().status).to.eql("PROCESSED");
    pm.expect(pm.response.json().status).to.not.eql("SAFE");
});
```

### Request 5: G4S Summary

```
Method: GET
URL:    http://localhost:8085/g4s/summary
```

Expected:
```json
{
    "SAFE":         2,
    "PROCESSED":    1,
    "PENDING_SAFE": 0,
    "CREATED":      0,
    "total":        3
}
```

### Request 6: Cleanup Test Data

```
Method: DELETE
URL:    http://localhost:8085/test/kafka/cleanup
```

---

## 9. RabbitMQ Management UI Guide

### Access

```
URL:      http://localhost:15672
Username: guest
Password: guest
```

### What to Watch During Testing

#### Queues Tab (http://localhost:15672/#/queues)

| Column | What it means |
|--------|--------------|
| Ready | Messages waiting to be consumed by G4S |
| Unacked | Messages G4S is currently processing |
| Total | Ready + Unacked |
| incoming | Messages/second arriving |
| deliver/get | Messages/second being consumed |
| ack | Messages/second being acknowledged |

**Expected behavior:**
- Send CASH → `cash.queue` Total briefly shows 1 → drops to 0 as G4S processes
- Send CARD → `cash.queue` stays at 0 — filter blocked it
- `cash.dlq` always 0 in happy path

#### Exchanges Tab (http://localhost:15672/#/exchanges)

Look for:
```
cash.exchange      direct  D  ← routes CASH to cash.queue
cash.dlq.exchange  direct  D  ← routes failures to cash.dlq
```

#### How to Peek at a Message Without Consuming It

1. Click on `cash.queue`
2. Scroll down to **Get messages**
3. Set Ack Mode to `Nack message requeue true`
4. Click **Get Message(s)**
5. Message is shown but stays in queue

---

## 10. Manual Test Cases

### TC-01: CASH transaction → full pipeline → SAFE ✅

**Steps:**
1. POST `{ transactionId: TC01-CASH, type: CASH, amount: 500 }`
2. Watch app terminal for:
   - `Filter received from Kafka: TC01-CASH type=CASH`
   - `CASH detected → routing to RabbitMQ`
   - `G4S received CASH transaction`
   - `SAFE confirmed: TC01-CASH`
3. Watch RabbitMQ UI — cash.queue briefly shows 1
4. GET `/g4s/transactions/TC01-CASH`

**Expected:** `status = SAFE`

---

### TC-02: CARD transaction → filtered → NOT SAFE ✅

**Steps:**
1. POST `{ transactionId: TC02-CARD, type: CARD, amount: 350 }`
2. Watch app terminal: `NON-CASH (CARD) → skipping G4S`
3. RabbitMQ cash.queue stays at 0
4. GET `/g4s/transactions/TC02-CARD`

**Expected:** `status = PROCESSED` (NOT SAFE)

---

### TC-03: GIFT_CERT → filtered → NOT SAFE ✅

**Steps:**
1. POST `{ transactionId: TC03-GIFT, type: GIFT_CERT, amount: 50 }`
2. GET `/g4s/transactions/TC03-GIFT`

**Expected:** `status = PROCESSED`

---

### TC-04: Mixed batch — only CASH becomes SAFE ✅

**Steps:**
1. POST TC04-CASH-A → CASH → 300.00
2. POST TC04-CARD-A → CARD → 150.00
3. POST TC04-CASH-B → CASH → 600.00
4. POST TC04-GIFT-A → GIFT_CERT → 25.00
5. Wait 10 seconds
6. GET all 4 transactions

**Expected:**
```
TC04-CASH-A → SAFE
TC04-CARD-A → PROCESSED
TC04-CASH-B → SAFE
TC04-GIFT-A → PROCESSED
```

---

### TC-05: Duplicate CASH — idempotency check ✅

**Steps:**
1. POST `{ transactionId: TC05-DUPE, type: CASH, amount: 500 }`
2. Wait 7 seconds → verify SAFE
3. POST same body again (same transactionId)
4. Wait 5 seconds
5. GET `/g4s/transactions/TC05-DUPE`

**Expected:**
- status = SAFE (not changed)
- amount = 500.0 (not doubled)
- App log shows: `Already SAFE, skipping: TC05-DUPE`

---

### TC-06: G4S Summary validation ✅

**Steps:**
1. GET `/g4s/summary`

**Expected:**
```json
{
    "SAFE":      N,
    "PROCESSED": M,
    "total":     N+M
}
```

---

## 11. MongoDB Validation Queries

```javascript
// Connect
docker exec -it mongodb mongosh
use kafkadb

// View all transactions
db.transactions.find().pretty()

// Count by status
db.transactions.aggregate([
  { $group: { _id: "$status", count: { $sum: 1 } } },
  { $sort: { _id: 1 } }
])

// Verify ONLY CASH transactions are SAFE
db.transactions.find({ status: "SAFE", type: { $ne: "CASH" } }).count()
// Expected: 0  (if not 0 — there is a routing bug!)

// All SAFE transactions
db.transactions.find({ status: "SAFE" }).pretty()

// All PROCESSED transactions
db.transactions.find({ status: "PROCESSED" }).pretty()

// Find stuck transactions (PENDING_SAFE for too long)
db.transactions.find({ status: "PENDING_SAFE" })

// Revenue secured today (CASH marked SAFE)
db.transactions.aggregate([
  { $match: { status: "SAFE", type: "CASH" } },
  { $group: {
      _id: "$storeId",
      totalCash: { $sum: "$amount" },
      count: { $sum: 1 }
  }},
  { $sort: { totalCash: -1 } }
])
```

---

## 12. Karate Automation

### File: `src/test/resources/karate/pipeline/cash-pipeline.feature`

```gherkin
Feature: CASH Transaction Pipeline — Kafka to G4S

  Background:
    * url baseUrl
    * header Content-Type = 'application/json'
    * def txnId = 'KARATE-CASH-' + java.lang.System.currentTimeMillis()

  @smoke @cash
  Scenario: CASH transaction flows through full pipeline to SAFE

    # Step 1: Publish to Kafka
    Given path '/test/kafka/publish/sales-transactions'
    And request
      """
      {
        "transactionId": "#(txnId)",
        "type":          "CASH",
        "amount":        500.00,
        "currency":      "USD",
        "storeId":       "STORE-001"
      }
      """
    When method POST
    Then status 200
    And match response.kafkaTopic == 'sales-transactions'
    And match response.type == 'CASH'

    # Step 2: Wait for Kafka → Filter → RabbitMQ → G4S
    * eval java.lang.Thread.sleep(7000)

    # Step 3: Verify SAFE
    Given path '/g4s/transactions/' + txnId
    When method GET
    Then status 200
    And match response.status == 'SAFE'
    And match response.type == 'CASH'
    And match response.amount == 500.0

  @regression @filter
  Scenario Outline: <type> transaction is filtered — NOT SAFE

    * def txnId = 'KARATE-' + '<type>' + '-' + java.lang.System.currentTimeMillis()

    Given path '/test/kafka/publish/sales-transactions'
    And request { transactionId: '#(txnId)', type: '<type>', amount: <amount> }
    When method POST
    Then status 200

    * eval java.lang.Thread.sleep(7000)

    Given path '/g4s/transactions/' + txnId
    When method GET
    Then status 200
    And match response.status == 'PROCESSED'
    And match response.status != 'SAFE'

    Examples:
      | type       | amount |
      | CARD       | 350.00 |
      | GIFT_CERT  | 50.00  |
      | CHECK      | 200.00 |
      | STORE_CARD | 75.00  |

  @e2e @regression
  Scenario: Mixed batch — only CASH becomes SAFE

    * def ts = java.lang.System.currentTimeMillis()
    * def cashId1 = 'KARATE-CASH-A-' + ts
    * def cardId  = 'KARATE-CARD-A-' + ts
    * def cashId2 = 'KARATE-CASH-B-' + ts
    * def giftId  = 'KARATE-GIFT-A-' + ts

    Given path '/test/kafka/publish/sales-transactions'
    And request { transactionId: '#(cashId1)', type: 'CASH',     amount: 300.00 }
    When method POST
    Then status 200

    Given path '/test/kafka/publish/sales-transactions'
    And request { transactionId: '#(cardId)',  type: 'CARD',     amount: 150.00 }
    When method POST
    Then status 200

    Given path '/test/kafka/publish/sales-transactions'
    And request { transactionId: '#(cashId2)', type: 'CASH',     amount: 600.00 }
    When method POST
    Then status 200

    Given path '/test/kafka/publish/sales-transactions'
    And request { transactionId: '#(giftId)',  type: 'GIFT_CERT', amount: 25.00 }
    When method POST
    Then status 200

    * eval java.lang.Thread.sleep(10000)

    Given path '/g4s/transactions/' + cashId1
    When method GET
    Then status 200
    And match response.status == 'SAFE'

    Given path '/g4s/transactions/' + cardId
    When method GET
    Then status 200
    And match response.status == 'PROCESSED'

    Given path '/g4s/transactions/' + cashId2
    When method GET
    Then status 200
    And match response.status == 'SAFE'

    Given path '/g4s/transactions/' + giftId
    When method GET
    Then status 200
    And match response.status == 'PROCESSED'
```

### Run Karate Pipeline Tests

```cmd
# App must be running first
mvn test -Dtest=KarateRunner
```

---

## 13. QA Testing Coverage

| Test Type | Tool | What's Validated |
|-----------|------|-----------------|
| API / Integration | Karate | End-to-end order + transaction flows |
| Manual Exploratory | Postman | HTTP bridge, routing verification |
| Data Integrity | MongoDB queries | ONLY CASH = SAFE, all others = PROCESSED |
| Visual Verification | RabbitMQ UI | Queue depth, routing confirmation |
| DLQ Testing | Karate + RabbitMQ | Dead letter queue on failure |
| Idempotency | Karate | Duplicate message handling |

---

## 14. Common Errors and Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| `%0A` in URL → 500 error | Newline copied into Postman URL | Type URL manually — never copy paste |
| `Unknown magic byte` | Avro deserializer reading JSON message | Add `containerFactory = "stringKafkaListenerContainerFactory"` to `@KafkaListener` |
| `Failed to convert Message content` | Jackson cannot serialize `LocalDateTime` | Add `jackson-datatype-jsr310` and register `JavaTimeModule` in `RabbitMQConfig` |
| `No qualifying bean of type KafkaTemplate<String, Order>` | Multiple Kafka templates conflict | Add `@Primary` to Avro KafkaTemplate bean |
| `status = PENDING_SAFE` stuck | RabbitMQ was down when filter tried to send | Restart app — G4S will process on next message |
| `status = CREATED` after sending | Old failed message — pipeline did not complete | Send a new message with fresh transactionId |
| `cash.queue` never gets messages | Filter consumer not routing correctly | Check `SalesTransactionConsumer` — verify CASH check is correct |
| G4S processes same transaction twice | Missing idempotency check | Check `G4SManagerService` — `Already SAFE, skipping` log should appear |

---

## 15. Key Learnings

### Content-Based Routing
- One Kafka topic receives ALL transaction types
- Consumer reads the content (type field) and decides the route
- CASH → RabbitMQ → G4S
- Non-CASH → mark PROCESSED and stop
- Keeps the producer simple — it does not need to know about G4S

### Kafka vs RabbitMQ — When to Use Which
- Kafka: broad ingestion, high volume, audit trail, replay capability
- RabbitMQ: targeted routing, task queues, workflow steps, push delivery
- Both together = best of both worlds (common in enterprise systems)

### Idempotency in Event-Driven Systems
- The same message can arrive twice (Kafka retry, consumer restart)
- Always check before processing: is this already done?
- In G4S: check if status is already SAFE before updating
- Without idempotency: duplicate processing, wrong data, financial errors

### Serialization Across Systems
- Kafka (sales-transactions): JSON String serializer
- Kafka (orders): Avro binary serializer
- RabbitMQ: Jackson JSON with JavaTimeModule for LocalDateTime
- Never mix serializers on the same topic

### Testing Strategy
- Postman: HTTP bridge to publish events — tests the full pipeline visually
- RabbitMQ UI: visual proof of message routing — see queue depth change live
- MongoDB queries: data integrity validation — ONLY CASH is SAFE
- Karate: automated end-to-end — repeatable on every build

---

## 16. Interview Q&A

### Q: Explain your Kafka + RabbitMQ architecture.

> We had a hybrid messaging system. Kafka was the broad ingestion layer —
> ALL sales transactions came in through Kafka regardless of payment type.
> This gave us high throughput, replay capability, and a complete audit trail.
>
> RabbitMQ was the narrow business routing layer. A Kafka consumer — our
> Filter Service — read every transaction and applied a business rule:
> if type is CASH, route to RabbitMQ cash.queue for G4S processing.
> Non-CASH transactions were marked PROCESSED and stopped there.
>
> The G4S Manager Service consumed from RabbitMQ and updated CASH
> transactions to SAFE status, confirming the physical cash was secured.
> This is called content-based routing — a common enterprise pattern.

---

### Q: Why use both Kafka AND RabbitMQ? Why not just one?

> Kafka and RabbitMQ solve different problems.
> Kafka is a distributed log — messages are kept forever and any consumer
> can replay them at any time. It handles millions of messages per second
> and is ideal for event sourcing and audit trails.
>
> RabbitMQ is a message broker — messages are delivered once and gone.
> It is better for task queues and specific workflow steps where you need
> push delivery and built-in retry logic.
>
> In our system: Kafka handled ALL transactions for ingestion and audit,
> RabbitMQ handled ONLY CASH transactions for the G4S workflow.
> Combining them gave us the best of both worlds.

---

### Q: How did you test this pipeline?

> I used a layered testing approach:
>
> First — Postman as an HTTP bridge to publish events to Kafka via our
> REST controller, since Postman cannot connect to Kafka directly.
>
> Second — RabbitMQ Management UI to visually verify routing.
> When I sent a CASH transaction I watched the cash.queue message count
> briefly hit 1 then drop to 0 as G4S processed it. For CARD transactions
> the queue stayed at 0 — proving the filter worked correctly.
>
> Third — MongoDB queries to validate data integrity.
> I ran aggregations to confirm ONLY CASH transactions had status SAFE
> and all other types had status PROCESSED.
>
> Fourth — Karate automation for repeatable end-to-end tests including
> happy path, all non-CASH types, mixed batch, and idempotency scenarios.

---

### Q: What is idempotency and why does it matter here?

> Idempotency means: processing the same message multiple times produces
> the same result as processing it once.
>
> In Kafka, the same message can be delivered more than once — for example
> if the consumer restarts mid-processing. Without idempotency in G4S,
> the same CASH transaction could be marked SAFE multiple times, potentially
> causing issues in reporting or triggering duplicate physical cash collections.
>
> I implemented idempotency in G4SManagerService by checking: is this
> transaction already SAFE? If yes, log a warning and skip. This is
> called the "check before update" pattern and is essential in any
> event-driven financial system.

---

## 17. Quick Reference Commands

### Start everything

```cmd
# Verify Docker containers
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# Start app
cd C:\kafka-avro-demo
mvn spring-boot:run
```

### Test via PowerShell (alternative to Postman)

```powershell
# Send CASH
Invoke-RestMethod -Method POST `
  -Uri "http://localhost:8085/test/kafka/publish/sales-transactions" `
  -ContentType "application/json" `
  -Body '{"transactionId":"TEST-CASH-001","type":"CASH","amount":500.00,"currency":"USD","storeId":"STORE-001"}'

# Send CARD
Invoke-RestMethod -Method POST `
  -Uri "http://localhost:8085/test/kafka/publish/sales-transactions" `
  -ContentType "application/json" `
  -Body '{"transactionId":"TEST-CARD-001","type":"CARD","amount":350.00,"currency":"USD","storeId":"STORE-001"}'

# Check status
Invoke-RestMethod -Uri "http://localhost:8085/g4s/transactions/TEST-CASH-001"
Invoke-RestMethod -Uri "http://localhost:8085/g4s/transactions/TEST-CARD-001"

# G4S summary
Invoke-RestMethod -Uri "http://localhost:8085/g4s/summary"

# Cleanup
Invoke-RestMethod -Method DELETE -Uri "http://localhost:8085/test/kafka/cleanup"
```

### MongoDB

```cmd
docker exec -it mongodb mongosh
use kafkadb
db.transactions.find().pretty()
db.transactions.find({ status: "SAFE" }).count()
db.transactions.find({ status: { $ne: "SAFE", $ne: "PROCESSED" } })
```

### RabbitMQ Management API

```cmd
# Queue status
curl -u guest:guest http://localhost:15672/api/queues/%2F/cash.queue

# DLQ status
curl -u guest:guest http://localhost:15672/api/queues/%2F/cash.dlq

# All queues
curl -u guest:guest http://localhost:15672/api/queues
```

### Kafka

```cmd
# List topics (should include sales-transactions)
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Check consumer groups
docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group filter-group --describe
```

---

## Final Summary

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Ingress | REST API (Spring Boot) | Accepts all sales transactions |
| Event Streaming | Kafka (sales-transactions topic) | Stores ALL transactions |
| Filter | SalesTransactionConsumer | Routes CASH → RabbitMQ |
| Message Broker | RabbitMQ (cash.queue) | Delivers CASH to G4S |
| G4S Service | G4SManagerService | Updates CASH to SAFE |
| Database | MongoDB (transactions collection) | Stores final state |
| Test Tool | Postman + RabbitMQ UI | Manual testing and verification |
| Automation | Karate feature files | Automated end-to-end tests |

**Pipeline: Postman → Kafka → Filter → RabbitMQ → G4S → SAFE ✅**

---

*Author: Japendra Reddy Seethammagari*

