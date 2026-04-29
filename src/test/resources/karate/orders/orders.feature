Feature: Orders API Tests

  Background:
    * url baseUrl
    * header Content-Type = 'application/json'

  Scenario: Send a valid order successfully
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-001",
        "amount": 250.0,
        "currency": "USD",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 201
    And match response.id == 'KARATE-001'
    And match response.amount == 250.0
    And match response.currency == 'USD'
    And match response.status == 'CREATED'
    And match response.message == 'Order processed successfully'
    And match response.timestamp == '#number'

  Scenario: Send order with default currency USD
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-002",
        "amount": 500.0,
        "currency": "USD",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 201
    And match response.id == 'KARATE-002'
    And match response.currency == 'USD'

  Scenario: Send order with EUR currency
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-003",
        "amount": 750.0,
        "currency": "EUR",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 201
    And match response.currency == 'EUR'
    And match response.amount == 750.0

  Scenario: Send order with PROCESSED status
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-004",
        "amount": 100.0,
        "currency": "GBP",
        "status": "PROCESSED"
      }
      """
    When method POST
    Then status 201
    And match response.status == 'PROCESSED'

  Scenario: Reject order with missing ID
    Given path '/orders'
    And request
      """
      {
        "id": "",
        "amount": 100.0,
        "currency": "USD",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 400

  Scenario: Get an existing order by ID
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-GET-001",
        "amount": 999.0,
        "currency": "USD",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 201

    * eval java.lang.Thread.sleep(3000)

    Given path '/orders/KARATE-GET-001'
    When method GET
    Then status 200
    And match response.id == 'KARATE-GET-001'
    And match response.amount == 999.0
    And match response.currency == 'USD'
    And match response.status == 'CREATED'
    And match response.receivedAt == '#string'

  Scenario: Get non-existent order returns 404
    Given path '/orders/NON-EXISTENT-999'
    When method GET
    Then status 404

  Scenario: Get all orders returns list with total
    Given path '/orders'
    When method GET
    Then status 200
    And match response.total == '#number'
    And match response.orders == '#array'

  Scenario: Get total count of orders
    Given path '/orders/count'
    When method GET
    Then status 200
    And match response.total == '#number'

  Scenario: Get orders filtered by status
    Given path '/orders/status/CREATED'
    When method GET
    Then status 200
    And match response == '#array'

  Scenario: Get orders filtered by currency
    Given path '/orders/currency/USD'
    When method GET
    Then status 200
    And match response == '#array'

  Scenario: Update order status
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-PATCH-001",
        "amount": 450.0,
        "currency": "USD",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 201

    * eval java.lang.Thread.sleep(3000)

    Given path '/orders/KARATE-PATCH-001/status'
    And param status = 'PROCESSED'
    When method PATCH
    Then status 200
    And match response.status == 'PROCESSED'
    And match response.id == 'KARATE-PATCH-001'

  Scenario: Retry an existing order
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-RETRY-001",
        "amount": 300.0,
        "currency": "USD",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 201

    * eval java.lang.Thread.sleep(3000)

    Given path '/orders/KARATE-RETRY-001/retry'
    When method POST
    Then status 200
    And match response.message == 'Order retried: KARATE-RETRY-001'

  Scenario: Retry non-existent order returns 404
    Given path '/orders/NON-EXISTENT-999/retry'
    When method POST
    Then status 404

  Scenario: Delete an existing order
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-DELETE-001",
        "amount": 150.0,
        "currency": "USD",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 201

    * eval java.lang.Thread.sleep(3000)

    Given path '/orders/KARATE-DELETE-001'
    When method DELETE
    Then status 200
    And match response.message == 'Order deleted: KARATE-DELETE-001'

  Scenario: Delete non-existent order returns 404
    Given path '/orders/NON-EXISTENT-999'
    When method DELETE
    Then status 404

  Scenario: Send bad order with negative amount is accepted by API
    Given path '/orders'
    And request
      """
      {
        "id": "KARATE-BAD-001",
        "amount": -99.99,
        "currency": "USD",
        "status": "CREATED"
      }
      """
    When method POST
    Then status 201
    And match response.id == 'KARATE-BAD-001'