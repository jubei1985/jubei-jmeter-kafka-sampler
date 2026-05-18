package ru.jmeter.sampler

import org.apache.jmeter.samplers.SampleResult
import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class KafkaProducerSSL {
    private static final Object lock = new Object()

    // Карты для хранения конфигураций и продюсеров по имени
    private static final Map<String, Properties> configs = [:]
    private static final Map<String, KafkaProducer<String, byte []>> producers = [:]
    private static final Map<String, RecordMetadata> lastMetadataMap = new ConcurrentHashMap<>()

    private static final Logger log = Logger.getLogger(KafkaProducerSSL.class.name)

    private KafkaProducerSSL() {}

    /**
     * Инициализация продюсера по имени
     */
    static void init(
            String producerName,
            String bootstrapServers,
            String keystorePath,
            String keystorePassword,
            String truststorePath,
            String truststorePassword){

        if (configs.containsKey(producerName)) return

        synchronized (lock) {
            if (configs.containsKey(producerName)) return

            Properties props = new Properties()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.name)
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.name) //StringSerializer.class.name)
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.name)
            // Можно заменить рандробин на липкость к партициям
//            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.internals.DefaultPartitioner")

            // SSL/mTLS
            props.put("security.protocol", "SSL")
            props.put("ssl.keystore.location", keystorePath)
            props.put("ssl.keystore.password", keystorePassword)
            props.put("ssl.truststore.location", truststorePath)
            props.put("ssl.truststore.password", truststorePassword)
            props.put("ssl.endpoint.identification.algorithm", "") // только для тестов

            // Настройки надёжности
            props.put(ProducerConfig.ACKS_CONFIG, "1")
            props.put(ProducerConfig.RETRIES_CONFIG, "3")
            props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000")
            props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000")

            try {
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)
                configs.put(producerName, props)
                producers.put(producerName, producer)
                log.info("KafkaProducerSSL: initialized '$producerName' for $bootstrapServers")
            } catch (Exception e) {
                log.error("Error creating producer '$producerName': ${e.message}", e)
                throw e
            }
        }
    }

    /**
     * Отправка сообщения через именованный продюсер
     */
    static SampleResult send(String producerName, String topic, String key, String header, Object value, Boolean typeData) {
        return sendInternal(producerName, topic, key, header, value, typeData)
    }

    private static SampleResult sendInternal(String producerName, String topic, String key, String header, Object value, Boolean typeData) {

        key = key?.trim()
        if (key == '') key = null

        header = header?.trim()
        if (header == '') header = null

        def res = new SampleResult()
        res.sampleStart()

        try {
            if (!producerName?.trim()) {
                return createFailedResult(topic, key, value.toString(), "NO_PRODUCER_NAME", "Not name for producer", res)
            }

            if (!producers.containsKey(producerName)) {
                return createFailedResult(topic, key, value.toString(), "NOT_INITIALIZED", "Producer: '$producerName' not initialized", res)
            }

            // Подготовка полезной нагрузки (Payload)
            byte[] payload
            if (value instanceof byte[]) {
                payload = (byte[]) value
            } else if (value != null) {
                String strValue = value.toString()
                // Если флаг typeData == true, считаем что пришла строка Base64
                if (typeData) {
                    try {
                        payload = Base64.getDecoder().decode(strValue.trim())
                    } catch (Exception e) {
                        log.error("Failed to decode Base64: ${e.message}")
                        payload = strValue.getBytes(StandardCharsets.UTF_8)
                    }
                } else {
                    payload = strValue.getBytes(StandardCharsets.UTF_8)
                }
            } else {
                payload = new byte[0]
            }

            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, payload)

            // Обработка заголовков
            if (header) {
                String[] lines = header.split('\n')
                for (int i = 0; i < lines.length; i++) {
                    String line = lines[i].trim()
                    if (!line || line.startsWith('#')) continue

                    def parts = line.contains('=') ? line.split('=', 2) : line.split(':', 2)
                    if (parts.length < 2) continue

                    def k = parts[0]?.trim()
                    def v = parts[1]?.trim()
                    if (k) {
                        record.headers().add(k, (v ?: '').getBytes(StandardCharsets.UTF_8))
                    }
                }
            }

            // Отправка (продюсер должен быть инициализирован как KafkaProducer<String, byte[]>)
            def future = producers.get(producerName).send(record)
            RecordMetadata metadata = future.get(30, TimeUnit.SECONDS)

            lastMetadataMap.put(producerName, metadata)

            res.successful = true
            res.responseCode = "200"
            res.responseMessage = "topic: ${metadata.topic()}, partition: ${metadata.partition()}, " +
                    "offset: ${metadata.offset()}, producer: $producerName, key: $key, " +
                    "value_type: ${typeData ? "Avro/Base64" : "String/JSON"}, payload_size: ${payload.length} bytes"

            return res

        } catch (Exception e) {
            log.error("Error send to Kafka: ${e.message}", e)
            return createFailedResult(topic, key, value.toString(), "SEND_FAILED", e.message, res)
        } finally {
            res.sampleEnd()
        }
    }


    /**
     * Закрытие конкретного продюсера
     */
    static SampleResult close(String producerName) {
        synchronized (lock) {
            def res = new SampleResult()

            if (producerName == null || !producers.containsKey(producerName)) {
                log.warn("Producer: '$producerName' not found for close")
                return createFailedResult(producerName, null, null, "PRODUCER_NULL", null, res)
            }

            KafkaProducer<String, byte[]> producer = producers.remove(producerName)
            Properties props = configs.remove(producerName)
            lastMetadataMap.remove(producerName)

            try {
                producer.close(Duration.ofSeconds(30))
                log.info("KafkaProducerSSL: Producer: '$producerName' closing")
                res.sampleStart()
                res.successful = true
                res.responseCode = "200"
                res.responseMessage = "KafkaProducerSSL: Producer: '$producerName' closing"
                return res
            } catch (Exception e) {
                log.warn("Error closed producer '$producerName': ${e.message}", e)
                return createFailedResult(producerName, null, null, "ERROR_CLOSE", e.message, res)
            } finally {
                res.sampleEnd()
            }

        }
    }

    /**
     * Закрытие всех продюсеров
     */

    static SampleResult closeAll() {
        def res = new SampleResult()
        res.sampleStart()

        StringBuilder sb = new StringBuilder("Close Report: ")
        boolean allOk = true

        // Работаем напрямую с ключами
        for (String name : producers.keySet().toArray(new String[0])) {
            def closeRes = close(name)
            if (!closeRes.isSuccessful()) {
                allOk = false
                sb.append(" [FAIL: $name] ")
            } else {
                sb.append(" [OK: $name] ")
            }
        }

        res.successful = allOk
        res.responseCode = allOk ? "200" : "500"
        res.responseMessage = sb.toString()
        res.sampleEnd()
        return res
    }

    private static SampleResult createFailedResult(String topic, String key, String value,
                                                   String code, String error, SampleResult res) {
        res.successful = false
        res.responseCode = code
        res.responseMessage = "topic: $topic, partition: ERROR, offset: -1, result: FAILED, " +
                "code: $code, error: $error, key: $key, value: $value"
        return res
    }

    /**
     * Получение последних метаданных по имени продюсера
     */
    static RecordMetadata getLastMetadata(String producerName) {
        return lastMetadataMap.get(producerName)
    }

    /**
     * Проверка, инициализирован ли продюсер
     */
    static boolean isInitialized(String producerName) {
        return producers.containsKey(producerName)
    }

    /**
     * Получение списка имён продюсеров
     */
    static Set<String> getProducerNames() {
        return new HashSet<>(producers.keySet())
    }
}
