@regression @loyalty @sanity
Feature: POS Loyalty Points Testing
# =====================================================================
# WHAT THIS TESTS:
#   Loyalty card scan, points earn, points redeem
#   POST /pos/loyalty → Kafka pos-loyalty-events topic
#
# RETAIL CONTEXT:
#   Customer scans loyalty card at POS
#   Points earned = based on purchase amount (1 point per £1)
#   Points redeemed = customer uses points for discount
#   pointsAfter = pointsBefore + pointsEarned - pointsRedeemed
# =====================================================================

  Background:
    * url baseUrl
    * header Content-Type = 'application/json'
    * def KafkaUtil = Java.type('kafka.KafkaAvroConsumerUtil')
    * def txnId     = 'LOY-TXN-' + java.lang.System.currentTimeMillis()
    * def loyaltyId = 'LOY-' + java.lang.System.currentTimeMillis()


  # =================================================================
  # SCENARIO 1: Earn loyalty points
  # @smoke @critical → runs in every pipeline
  # =================================================================
  @smoke @critical
  Scenario: Customer earns loyalty points on purchase

    Given path '/pos/loyalty'
    And request
      """
      {
        "transactionId":  "#(txnId)",
        "loyaltyId":      "#(loyaltyId)",
        "eventType":      "EARN",
        "pointsBefore":   500,
        "pointsEarned":   45,
        "pointsRedeemed": 0
      }
      """
    When method POST
    Then status 201

    And match response.loyaltyId    == loyaltyId
    And match response.pointsBefore == 500
    And match response.pointsEarned == 45
    And match response.pointsAfter  == 545
    And match response.eventType    == 'EARN'

    * print 'STEP 1 PASSED: API calculated points correctly'

    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-loyalty-events', loyaltyId, 10)

    * match event != null
    * match event.loyaltyId      == loyaltyId
    * match event.eventType      == 'EARN'
    * match event.pointsBefore   == 500
    * match event.pointsEarned   == 45
    * match event.pointsRedeemed == 0
    * match event.pointsAfter    == 545

    # Business rule: pointsAfter = pointsBefore + pointsEarned - pointsRedeemed
    * assert event.pointsAfter == event.pointsBefore + event.pointsEarned - event.pointsRedeemed

    * print 'STEP 2 PASSED: Loyalty earn event verified in Kafka'


  # =================================================================
  # SCENARIO 2: Redeem loyalty points
  # =================================================================
  @regression @loyalty
  Scenario: Customer redeems loyalty points for discount

    Given path '/pos/loyalty'
    And request
      """
      {
        "transactionId":  "#(txnId)",
        "loyaltyId":      "#(loyaltyId)",
        "eventType":      "REDEEM",
        "pointsBefore":   545,
        "pointsEarned":   0,
        "pointsRedeemed": 100
      }
      """
    When method POST
    Then status 201

    And match response.pointsBefore   == 545
    And match response.pointsRedeemed == 100
    And match response.pointsAfter    == 445
    And match response.eventType      == 'REDEEM'

    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-loyalty-events', loyaltyId, 10)

    * match event.eventType      == 'REDEEM'
    * match event.pointsRedeemed == 100
    * assert event.pointsAfter   < event.pointsBefore

    * print 'PASSED: Loyalty redeem event verified in Kafka'


  # =================================================================
  # SCENARIO 3: Schema validation
  # =================================================================
  @regression @loyalty
  Scenario: Loyalty event schema validation

    Given path '/pos/loyalty'
    And request
      """
      {
        "transactionId":  "#(txnId)",
        "loyaltyId":      "#(loyaltyId)",
        "eventType":      "EARN",
        "pointsBefore":   100,
        "pointsEarned":   10,
        "pointsRedeemed": 0
      }
      """
    When method POST
    Then status 201

    * java.lang.Thread.sleep(2000)

    * def event = KafkaUtil.getMessage('pos-loyalty-events', loyaltyId, 10)

    * match event contains
      """
      {
        transactionId:  '#string',
        loyaltyId:      '#string',
        eventType:      '#string',
        pointsBefore:   '#number',
        pointsEarned:   '#number',
        pointsRedeemed: '#number',
        pointsAfter:    '#number',
        timestamp:      '#number'
      }
      """

    * assert event.pointsAfter    >= 0
    * assert event.pointsBefore   >= 0
    * assert event.pointsEarned   >= 0
    * assert event.pointsRedeemed >= 0

    * print 'PASSED: Loyalty schema validation'


  # =================================================================
  # SCENARIO 4: Earn and redeem in same session
  # @slow → skipped in PR pipeline
  # =================================================================
  @regression @loyalty @slow
  Scenario: Customer earns then redeems points in same visit

    * def memberId = 'LOY-MEMBER-' + java.lang.System.currentTimeMillis()

    # Step 1: Earn points
    * def earnTxnId = 'EARN-' + java.lang.System.currentTimeMillis()

    Given path '/pos/loyalty'
    And request
      """
      {
        "transactionId":  "#(earnTxnId)",
        "loyaltyId":      "#(memberId)",
        "eventType":      "EARN",
        "pointsBefore":   200,
        "pointsEarned":   50,
        "pointsRedeemed": 0
      }
      """
    When method POST
    Then status 201
    * def afterEarn = response.pointsAfter

    * print 'After earn:', afterEarn

    # Step 2: Redeem points
    * def redeemTxnId = 'REDEEM-' + java.lang.System.currentTimeMillis()

    Given path '/pos/loyalty'
    And request
      """
      {
        "transactionId":  "#(redeemTxnId)",
        "loyaltyId":      "#(memberId)",
        "eventType":      "REDEEM",
        "pointsBefore":   #(afterEarn),
        "pointsEarned":   0,
        "pointsRedeemed": 50
      }
      """
    When method POST
    Then status 201
    * def finalBalance = response.pointsAfter

    * print 'Final balance:', finalBalance

    # Net change = 0 (earned 50 then redeemed 50)
    * assert finalBalance == 200

    * print 'PASSED: Earn and redeem verified — net zero points change'