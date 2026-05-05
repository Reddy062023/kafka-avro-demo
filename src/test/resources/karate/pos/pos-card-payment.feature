@regression @payment @sanity
Feature: POS Card Payment Testing
# =====================================================================
# WHAT THIS TESTS:
#   Card payment events — VISA, debit, gift card, store card
#   POST /pos/payments → Kafka pos-payment-events topic
#
# RETAIL CONTEXT:
#   Customer taps/swipes card → terminal sends to payment gateway
#   Gateway approves/declines → event published to Kafka
#   cashBackAmount > 0 only for debit cards
# =====================================================================

  Background:
    * url baseUrl
    * header Content-Type = 'application/json'
    * def KafkaUtil = Java.type('kafka.KafkaAvroConsumerUtil')
    * def txnId = 'PAY-' + java.lang.System.currentTimeMillis()


  # =================================================================
  # SCENARIO 1: VISA card approved
  # @smoke @critical → runs in every pipeline
  # =================================================================
  @smoke @critical
  Scenario: VISA card payment approved and event published to Kafka

    Given path '/pos/payments'
    And request
      """
      {
        "transactionId":  "#(txnId)",
        "cardType":       "VISA",
        "maskedPAN":      "****1234",
        "amount":         75.50,
        "authCode":       "AUTH456",
        "approved":       true,
        "failureReason":  "",
        "cashBackAmount": 0.0
      }
      """
    When method POST
    Then status 201

    And match response.approved == true
    And match response.cardType == 'VISA'
    And match response.authCode == 'AUTH456'

    * print 'STEP 1 PASSED: API accepted VISA payment'

    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-payment-events', txnId, 10)

    * match event != null
    * match event.transactionId   == txnId
    * match event.cardType        == 'VISA'
    * match event.maskedPAN       == '****1234'
    * match event.approved        == true
    * match event.authCode        == 'AUTH456'
    * assert event.amount         > 0
    * assert event.cashBackAmount == 0.0

    * print 'STEP 2 PASSED: VISA payment event verified in Kafka'


  # =================================================================
  # SCENARIO 2: Debit card with cash back
  # =================================================================
  @regression @payment
  Scenario: Debit card payment with cash back

    Given path '/pos/payments'
    And request
      """
      {
        "transactionId":  "#(txnId)",
        "cardType":       "DEBIT",
        "maskedPAN":      "****5678",
        "amount":         120.00,
        "authCode":       "AUTH789",
        "approved":       true,
        "failureReason":  "",
        "cashBackAmount": 20.00
      }
      """
    When method POST
    Then status 201

    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-payment-events', txnId, 10)

    * match event.cardType        == 'DEBIT'
    * match event.approved        == true
    * assert event.cashBackAmount > 0
    * match event.cashBackAmount  == 20.0
    * assert event.cardType       == 'DEBIT'

    * print 'PASSED: Debit card with cash back verified'


  # =================================================================
  # SCENARIO 3: Schema validation — payment event fields
  # =================================================================
  @regression @payment
  Scenario: Payment event schema validation

    Given path '/pos/payments'
    And request
      """
      {
        "transactionId":  "#(txnId)",
        "cardType":       "GIFT_CARD",
        "maskedPAN":      "****9999",
        "amount":         30.00,
        "authCode":       "GC123",
        "approved":       true,
        "failureReason":  "",
        "cashBackAmount": 0.0
      }
      """
    When method POST
    Then status 201

    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-payment-events', txnId, 10)

    * match event contains
      """
      {
        transactionId:  '#string',
        cardType:       '#string',
        maskedPAN:      '#string',
        amount:         '#number',
        authCode:       '#string',
        approved:       '#boolean',
        failureReason:  '#string',
        cashBackAmount: '#number',
        timestamp:      '#number'
      }
      """

    * print 'PASSED: Payment event schema validation'


  # =================================================================
  # SCENARIO 4: Declined card payment
  # =================================================================
  @regression @payment
  Scenario: Declined card payment returns 402 and event published

    Given path '/pos/payments'
    And request
      """
      {
        "transactionId":  "#(txnId)",
        "cardType":       "VISA",
        "maskedPAN":      "****0000",
        "amount":         500.00,
        "authCode":       "",
        "approved":       false,
        "failureReason":  "INSUFFICIENT_FUNDS",
        "cashBackAmount": 0.0
      }
      """
    When method POST
    Then status 402

    And match response.approved      == false
    And match response.failureReason == 'INSUFFICIENT_FUNDS'

    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-payment-events', txnId, 10)

    * match event != null
    * match event.approved      == false
    * match event.failureReason == 'INSUFFICIENT_FUNDS'
    * assert event.amount       > 0

    * print 'PASSED: Declined payment event verified in Kafka'


  # =================================================================
  # SCENARIO 5: Data-driven — multiple card types
  # @slow → skipped in PR pipeline
  # =================================================================
  @regression @payment @slow
  Scenario Outline: <cardType> card payment processed correctly

    Given path '/pos/payments'
    And request
      """
      {
        "transactionId":  "<cardType>-#(java.lang.System.currentTimeMillis())",
        "cardType":       "<cardType>",
        "maskedPAN":      "****1111",
        "amount":         <amount>,
        "authCode":       "AUTH001",
        "approved":       true,
        "cashBackAmount": 0.0
      }
      """
    When method POST
    Then status 201
    And match response.approved == true
    And match response.cardType == '<cardType>'

    * print 'PASSED: <cardType> payment processed'

    Examples:
      | cardType   | amount |
      | VISA       | 25.99  |
      | MASTERCARD | 45.00  |
      | AMEX       | 120.50 |
      | STORE_CARD | 15.00  |