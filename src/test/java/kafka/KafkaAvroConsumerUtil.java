package kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.*;

/**
 * KafkaAvroConsumerUtil — Bridge between Kafka/Avro and Karate.
 *
 * SUPPORTS TWO ENVIRONMENTS:
 *   local   → no SSL, no SASL (your docker setup)
 *             bootstrap.servers = localhost:9092
 *             security.protocol = PLAINTEXT
 *
 *   staging → SSL + SASL (production-like)
 *             bootstrap.servers = kafka-broker:9093
 *             security.protocol = SASL_SSL
 *
 * ENVIRONMENT is controlled by system property:
 *   mvn test -Dkarate.env=local    → no SSL
 *   mvn test -Dkarate.env=staging  → SSL + SASL
 *
 * CREDENTIALS always come from environment variables — never hardcoded:
 *   KAFKA_BOOTSTRAP  = kafka host:port
 *   KAFKA_USER       = SASL username
 *   KAFKA_PASS       = SASL password
 *   TRUSTSTORE_PATH  = path to truststore.jks
 *   TRUSTSTORE_PASS  = truststore password
 *   KEYSTORE_PATH    = path to keystore.jks (mTLS only)
 *   KEYSTORE_PASS    = keystore password (mTLS only)
 *   SCHEMA_REGISTRY  = schema registry URL
 */
public class KafkaAvroConsumerUtil {

    // ─── Read environment ─────────────────────────────────────────────
    // System.getenv() reads environment variables set in OS or CI/CD
    // System.getProperty() reads -D flags passed to Maven
    private static final String ENV =
        System.getProperty("karate.env", "local");

    private static final String BOOTSTRAP =
        System.getenv("KAFKA_BOOTSTRAP") != null
            ? System.getenv("KAFKA_BOOTSTRAP")
            : "localhost:9092";   // default for local

    private static final String SCHEMA_REGISTRY =
        System.getenv("SCHEMA_REGISTRY") != null
            ? System.getenv("SCHEMA_REGISTRY")
            : "http://localhost:8081";  // default for local

    // ─────────────────────────────────────────────────────────────────
    // getMessage() — called from Karate feature file
    //
    // Arguments:
    //   topic      = Kafka topic name to read from
    //   orderId    = specific order ID to find (null = first message)
    //   timeoutSec = seconds to wait before giving up
    //
    // Returns:
    //   Java Map — Karate reads this exactly like JSON
    //   null     — if no message found within timeout
    // ─────────────────────────────────────────────────────────────────
    public static Map<String, Object> getMessage(
            String topic, String orderId, int timeoutSec) {

        Properties props = buildProperties();

        try (KafkaConsumer<String, Object> consumer =
                new KafkaConsumer<>(props)) {

            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + (timeoutSec * 1000L);

            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));

                for (var record : records) {
                    Map<String, Object> msg = toMap(record.value());

                    if (orderId == null ||
    orderId.equals(String.valueOf(msg.get("id"))) ||
    orderId.equals(String.valueOf(msg.get("transactionId"))) ||
    orderId.equals(String.valueOf(msg.get("loyaltyId")))) {
                        System.out.println("✅ Found: " + msg);
                        return msg;
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("❌ ERROR: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("⚠️ Not found: orderId=" + orderId);
        return null;
    }

    // ─────────────────────────────────────────────────────────────────
    // buildProperties() — builds Kafka config based on environment
    //
    // LOCAL  → plain connection, no SSL, no SASL
    // STAGING → SSL + SASL_PLAIN connection
    // ─────────────────────────────────────────────────────────────────
    private static Properties buildProperties() {
        Properties props = new Properties();

        // Common properties for ALL environments
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "karate-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Avro deserializer — same for all environments
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url",  SCHEMA_REGISTRY);
        props.put("specific.avro.reader", false);

        // Environment-specific security properties
        if ("staging".equals(ENV) || "prod".equals(ENV)) {
            System.out.println("🔒 Applying SSL + SASL config for env: " + ENV);
            applySSLandSASL(props);
        } else {
            System.out.println("🔓 Local mode — no SSL, no SASL");
            // PLAINTEXT — no security config needed for local
            props.put("security.protocol", "PLAINTEXT");
        }

        return props;
    }

    // ─────────────────────────────────────────────────────────────────
    // applySSLandSASL() — adds SSL + SASL properties
    //
    // SASL_SSL = both encryption (SSL) AND authentication (SASL)
    //
    // Credentials come from environment variables — never hardcoded
    // CI/CD pipeline sets these variables before running tests
    // ─────────────────────────────────────────────────────────────────
    private static void applySSLandSASL(Properties props) {

        // ── Security protocol ────────────────────────────────────────
        // SASL_SSL = use both SASL authentication AND SSL encryption
        // Other options: PLAINTEXT, SSL, SASL_PLAINTEXT
        props.put("security.protocol", "SASL_SSL");

        // ── SASL mechanism ───────────────────────────────────────────
        // PLAIN        = username + password (simplest)
        // SCRAM-SHA-256 = hashed password (more secure)
        // GSSAPI       = Kerberos (enterprise)
        props.put("sasl.mechanism", "PLAIN");

        // ── SASL credentials ─────────────────────────────────────────
        // Read from environment variables — NEVER hardcode passwords
        // In CI/CD: export KAFKA_USER=myuser && export KAFKA_PASS=mypassword
        String kafkaUser = System.getenv("KAFKA_USER");
        String kafkaPass = System.getenv("KAFKA_PASS");

        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule " +
            "required username=\"" + kafkaUser + "\" " +
            "password=\"" + kafkaPass + "\";");

        // ── SSL truststore ────────────────────────────────────────────
        // truststore.jks contains the CA certificate that signed
        // the Kafka broker's SSL certificate
        // Without this → SSLHandshakeException PKIX path building failed
        String truststorePath = System.getenv("TRUSTSTORE_PATH");
        String truststorePass = System.getenv("TRUSTSTORE_PASS");

        props.put("ssl.truststore.location", truststorePath);
        props.put("ssl.truststore.password", truststorePass);

        // ── SSL keystore (mTLS only) ──────────────────────────────────
        // keystore.jks contains YOUR certificate + private key
        // Only needed for mTLS — when broker requires client certificate
        // Regular TLS does NOT need keystore — only truststore
        String keystorePath = System.getenv("KEYSTORE_PATH");
        String keystorePass = System.getenv("KEYSTORE_PASS");
        String keyPass      = System.getenv("KEY_PASS");

        if (keystorePath != null && !keystorePath.isEmpty()) {
            System.out.println("🔐 mTLS enabled — using keystore");
            props.put("ssl.keystore.location", keystorePath);
            props.put("ssl.keystore.password", keystorePass);
            props.put("ssl.key.password",      keyPass);
        } else {
            System.out.println("🔒 Regular TLS — truststore only");
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // toMap() — converts Avro GenericRecord to plain Java Map
    //
    // CRITICAL: Avro stores strings as Utf8 type
    // Karate sees Utf8 as OTHER — not STRING
    // .toString() converts Utf8 → java.lang.String
    // Without this: match message.id == '#string' FAILS
    // ─────────────────────────────────────────────────────────────────
    private static Map<String, Object> toMap(Object avro) {
        Map<String, Object> result = new LinkedHashMap<>();

        if (avro instanceof GenericRecord rec) {
            rec.getSchema().getFields().forEach(f -> {
                Object val = rec.get(f.name());

                // Convert Avro Utf8 → plain Java String
                if (val != null &&
                    val.getClass().getName().contains("Utf8")) {
                    val = val.toString();
                }

                result.put(f.name(), val);
            });
        }

        return result;
    }
}