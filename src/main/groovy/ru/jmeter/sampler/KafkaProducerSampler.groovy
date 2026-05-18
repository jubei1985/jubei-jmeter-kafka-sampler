package ru.jmeter.sampler

import org.apache.jmeter.samplers.AbstractSampler
import org.apache.jmeter.samplers.Entry
import org.apache.jmeter.samplers.SampleResult

class KafkaProducerSampler extends AbstractSampler {

    // Ключи для сохранения в .jmx
    private static final String PRODUCER_NAME = "producerName"
    private static final String BOOTSTRAP = "bootstrapServers"
    private static final String KEYSTORE_PATH = "keystorePath"
    private static final String KEYSTORE_PASS = "keystorePassword"
    private static final String TRUSTSTORE_PATH = "truststorePath"
    private static final String TRUSTSTORE_PASS = "truststorePassword"
    private static final String TOPIC = "topic"
    private static final String KEY = "key"
    private static final String HEADER = "header"
    private static final String MESSAGE = "message"
    private static final String AVRO = "avro"
    private static final String CLOSE_ALL = "closeAll"
    private static final String CLOSE = "close"


    // Геттеры и сеттеры
    void setProducerName(String val) { setProperty(PRODUCER_NAME, val) }

    String getProducerName() { getPropertyAsString(PRODUCER_NAME) }

    void setBootstrapServers(String val) { setProperty(BOOTSTRAP, val) }

    String getBootstrapServers() { getPropertyAsString(BOOTSTRAP) }

    void setKeystorePath(String val) { setProperty(KEYSTORE_PATH, val) }

    String getKeystorePath() { getPropertyAsString(KEYSTORE_PATH) }

    void setKeystorePassword(String val) { setProperty(KEYSTORE_PASS, val) }

    String getKeystorePassword() { getPropertyAsString(KEYSTORE_PASS) }

    void setTruststorePath(String val) { setProperty(TRUSTSTORE_PATH, val) }

    String getTruststorePath() { getPropertyAsString(TRUSTSTORE_PATH) }

    void setTruststorePassword(String val) { setProperty(TRUSTSTORE_PASS, val) }

    String getTruststorePassword() { getPropertyAsString(TRUSTSTORE_PASS) }

    void setTopic(String val) { setProperty(TOPIC, val) }

    String getTopic() { getPropertyAsString(TOPIC) }

    void setKey(String val) { setProperty(KEY, val) }

    String getKey() { getPropertyAsString(KEY) }

    void setHeader(String val) { setProperty(HEADER, val) }

    String getHeader() { getPropertyAsString(HEADER) }

    void setMessage(String val) { setProperty(MESSAGE, val) }

    String getMessage() { getPropertyAsString(MESSAGE) }

    void setAvro(boolean val) { setProperty(AVRO, val) }

    boolean getAvro() { getPropertyAsBoolean(AVRO, false) }


    void setCloseAll(boolean val) { setProperty(CLOSE_ALL, val) }

    boolean getCloseAll() { getPropertyAsBoolean(CLOSE_ALL, false) }

    void setClose(boolean val) { setProperty(CLOSE, val) }
    boolean getClose() { getPropertyAsBoolean(CLOSE, false) }

    @Override
    SampleResult sample(Entry entry) {
        SampleResult result
        if (!getCloseAll() && !getClose()) {
            KafkaProducerSSL.init(
                    getProducerName(),
                    getBootstrapServers(),
                    getKeystorePath(),
                    getKeystorePassword(),
                    getTruststorePath(),
                    getTruststorePassword()
            )
            result = KafkaProducerSSL.send(getProducerName(), getTopic(), getKey(), getHeader(), getMessage(),getAvro())
        }
        else if (getClose()) {
            result = KafkaProducerSSL.close(getProducerName())
        } else {
            result = KafkaProducerSSL.closeAll()
        }
        this.setProperty(MESSAGE, "");
        this.setProperty(HEADER, "");
        result.setSampleLabel(getName())

        return result
    }
}
