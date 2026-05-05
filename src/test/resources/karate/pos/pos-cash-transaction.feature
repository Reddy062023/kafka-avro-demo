@regression @cash @sanity
Feature: POS Cash Transaction Testing
# =====================================================================
# WHAT THIS TESTS:
#   Complete cash sale flow at POS terminal
#   POST /pos/transactions → Kafka pos-transactions topic
#
# RETAIL CONTEXT:
#   Cashier scans items → customer pays cash → change given
#   Transaction published to Kafka as Avro message
#   KafkaAvroConsumerUtil reads it and returns to Karate as Map
#
# SCHEMAS:
#   Transaction.avsc → pos-transactions topic
#   Fields: transactionId, storeId, registerId,
#           totalAmount, taxAmount, paymentMethod,
#           status, cashierId, timestamp
# =====================================================================

  Background:
    * url baseUrl
    * header Content-Type = 'application/json'
    * def KafkaUtil = Java.type('kafka.KafkaAvroConsumerUtil')
    # Unique transaction ID per test run — never clashes
    * def txnId = 'TXN-' + java.lang.System.currentTimeMillis()


  # =================================================================
  # SCENARIO 1: Basic cash sale — prove Kafka event published
  # @smoke @critical → runs in every pipeline including PR
  # =================================================================
  @smoke @critical
  Scenario: Cash sale publishes event to pos-transactions topic

    Given path '/pos/transactions'
    And request
      """
      {
        "transactionId": "#(txnId)",
        "storeId":       "STORE-001",
        "registerId":    "REG-001",
        "totalAmount":   25.99,
        "taxAmount":     2.00,
        "paymentMethod": "CASH",
        "cashierId":     "C001"
      }
      """
    When method POST
    Then status 201

    And match response.transactionId == txnId
    And match response.status        == 'COMPLETED'
    And match response.message       == 'Transaction processed successfully'

    * print 'STEP 1 PASSED: API accepted cash transaction'

    # Wait for Kafka producer to finish sending before polling
    * java.lang.Thread.sleep(2000)

    # Read Kafka event from pos-transactions topic
    * def event = KafkaUtil.getMessage('pos-transactions', txnId, 10)

    * print 'Kafka event:', event

    * match event != null
    * match event.transactionId == txnId
    * match event.paymentMethod == 'CASH'
    * match event.status        == 'COMPLETED'
    * match event.storeId       == 'STORE-001'
    * assert event.totalAmount  > 0
    * assert event.timestamp    > 0

    * print 'STEP 2 PASSED: Kafka event verified for cash transaction'


  # =================================================================
  # SCENARIO 2: Schema validation — all fields correct types
  # @regression → runs in nightly full regression only
  # =================================================================
  @regression @cash
  Scenario: Cash transaction Kafka event has correct schema

    Given path '/pos/transactions'
    And request
      """
      {
        "transactionId": "#(txnId)",
        "storeId":       "STORE-001",
        "registerId":    "REG-002",
        "totalAmount":   99.50,
        "taxAmount":     7.50,
        "paymentMethod": "CASH",
        "cashierId":     "C002"
      }
      """
    When method POST
    Then status 201

    # Wait for Kafka producer to finish sending
    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-transactions', txnId, 10)

    # Validate every field type — catches schema drift
    * match event contains
      """
      {
        transactionId:  '#string',
        storeId:        '#string',
        registerId:     '#string',
        totalAmount:    '#number',
        taxAmount:      '#number',
        paymentMethod:  '#string',
        status:         '#string',
        cashierId:      '#string',
        timestamp:      '#number'
      }
      """

    * print 'PASSED: All schema fields correct types'


  # =================================================================
  # SCENARIO 3: Business rules — amount must be positive
  # =================================================================
  @regression @cash
  Scenario: Cash transaction total amount must be positive

    Given path '/pos/transactions'
    And request
      """
      {
        "transactionId": "#(txnId)",
        "storeId":       "STORE-001",
        "registerId":    "REG-001",
        "totalAmount":   55.75,
        "paymentMethod": "CASH",
        "cashierId":     "C001"
      }
      """
    When method POST
    Then status 201

    # Wait for Kafka producer to finish sending
    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-transactions', txnId, 10)

    * assert event.totalAmount  > 0
    * assert event.taxAmount    >= 0
    * assert event.timestamp    > 0

    * print 'PASSED: Business rules verified for cash transaction'


  # =================================================================
  # SCENARIO 4: Different stores and registers
  # @slow → skipped in PR pipeline, runs in nightly
  # =================================================================
  @regression @cash @slow
  Scenario Outline: Cash sale works across different stores

    Given path '/pos/transactions'
    And request
      """
      {
        "transactionId": "<txnPrefix>-#(java.lang.System.currentTimeMillis())",
        "storeId":       "<storeId>",
        "registerId":    "<registerId>",
        "totalAmount":   <amount>,
        "paymentMethod": "CASH",
        "cashierId":     "<cashierId>"
      }
      """
    When method POST
    Then status 201
    And match response.status  == 'COMPLETED'
    And match response.storeId == '<storeId>'

    * print 'PASSED: Cash sale for store <storeId>'

    Examples:
      | txnPrefix | storeId   | registerId | amount | cashierId |
      | TXN-S1    | STORE-001 | REG-001    | 15.99  | C001      |
      | TXN-S2    | STORE-002 | REG-003    | 45.50  | C005      |
      | TXN-S3    | STORE-003 | REG-007    | 120.00 | C010      |


  # =================================================================
  # SCENARIO 5: Negative — transaction not found in Kafka
  # =================================================================
  @regression @cash
  Scenario: Non-existent transaction returns null from Kafka

    * def fakeEvent = KafkaUtil.getMessage('pos-transactions', 'FAKE-TXN-999', 3)

    # Must return null — no such transaction in Kafka
    * match fakeEvent == null

    * print 'PASSED: Non-existent transaction correctly returns null'