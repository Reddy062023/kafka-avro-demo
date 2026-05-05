function fn() {
  var env = karate.env || 'local';
  karate.log('=== Karate Config ===');
  karate.log('Environment:', env);
  karate.log('Mode: PLAINTEXT — no SSL, no SASL');
  karate.log('Base URL: http://localhost:8085');
  karate.log('Schema Registry: http://localhost:8081');
  karate.log('SSL Enabled: false');
  karate.log('====================');
  var config = {
    baseUrl:        'http://localhost:8085',
    schemaRegistry: 'http://localhost:8081',
    kafkaBootstrap: 'localhost:9092',
    sslEnabled:     false
  };
  return config;
}