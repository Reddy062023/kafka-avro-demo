function fn() {
  var env = karate.env || 'local';

  var config = {
    baseUrl: 'http://localhost:8085'
  };

  karate.log('Environment:', env);
  karate.log('Base URL:', config.baseUrl);

  return config;
}