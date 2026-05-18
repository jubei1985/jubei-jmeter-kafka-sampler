package ru.jmeter.sampler

import org.apache.jmeter.gui.util.HorizontalPanel
import org.apache.jmeter.gui.util.VerticalPanel
import org.apache.jmeter.samplers.gui.AbstractSamplerGui
import org.apache.jmeter.testelement.TestElement
import org.apache.jorphan.gui.JLabeledPasswordField
import org.apache.jorphan.gui.JLabeledTextField

import javax.swing.*
import java.awt.*

public class KafkaProducerSamplerGui extends AbstractSamplerGui {

    private JLabeledTextField samplerNameField
    private JLabeledTextField producerName
    private JLabeledTextField bootstrapField
    private JLabeledTextField keystorePathField
    private JLabeledPasswordField keystorePassField
    private JLabeledTextField truststorePathField
    private JLabeledPasswordField truststorePassField
    private JLabeledTextField topicField
    private JLabeledTextField keyField
    private JTextArea headerArea
    private JTextArea messageArea

    private JCheckBox avro
    private JCheckBox closeAllBox
    private JCheckBox closeBox


    KafkaProducerSamplerGui() {
        init()
    }

    @Override
    void configure(TestElement element) {
        super.configure(element)
        samplerNameField.text = element.getName()
        producerName.text = element.getPropertyAsString("producerName")
        bootstrapField.text = element.getPropertyAsString("bootstrapServers")
        keystorePathField.text = element.getPropertyAsString("keystorePath")
        keystorePassField.text = element.getPropertyAsString("keystorePassword")
        truststorePathField.text = element.getPropertyAsString("truststorePath")
        truststorePassField.text = element.getPropertyAsString("truststorePassword")
        topicField.text = element.getPropertyAsString("topic")
        keyField.text = element.getPropertyAsString("key")
        headerArea.text = element.getPropertyAsString("header")
        messageArea.text = element.getPropertyAsString("message")
        avro.selected = element.getPropertyAsBoolean ("avro")
        closeAllBox.selected = element.getPropertyAsBoolean ("closeAll")
        closeBox.selected = element.getPropertyAsBoolean ("close")

    }

    @Override
    TestElement createTestElement() {
        KafkaProducerSampler sampler = new KafkaProducerSampler()
        modifyTestElement(sampler)
        return sampler
    }

    @Override
    void modifyTestElement(TestElement element) {
        super.configureTestElement(element)
        KafkaProducerSampler sampler = (KafkaProducerSampler) element
        sampler.setName(samplerNameField.text)
        sampler.setProducerName(producerName.text)
        sampler.setBootstrapServers(bootstrapField.text)
        sampler.setKeystorePath(keystorePathField.text)
        sampler.setKeystorePassword(keystorePassField.text)
        sampler.setTruststorePath(truststorePathField.text)
        sampler.setTruststorePassword(truststorePassField.text)
        sampler.setTopic(topicField.text)
        sampler.setKey(keyField.text)
        sampler.setHeader(headerArea.text)
        sampler.setMessage(messageArea.text)

        sampler.setAvro(avro.selected)
        sampler.setCloseAll(closeAllBox.selected)
        sampler.setClose(closeBox.selected)
    }

    @Override
    public String getLabelResource() {
        return "Jubei kafka producer"
    }
    @Override
    public String getStaticLabel() {
        return "Jubei kafka producer";
    }
    private void init() {
        setLayout(new BorderLayout(0, 5))
        setBorder(BorderFactory.createTitledBorder("Jubei kafka producer with avro settings"))
        JPanel mainPanel = new VerticalPanel()
        samplerNameField = new JLabeledTextField("Sampler Name")
        producerName = new JLabeledTextField("Name for producer")
        bootstrapField = new JLabeledTextField("Bootstrap Servers (host:port)")
        keystorePathField = new JLabeledTextField("Keystore Path")
        keystorePassField = new JLabeledPasswordField("Keystore Password")
        truststorePathField = new JLabeledTextField("Truststore Path")
        truststorePassField = new JLabeledPasswordField("Truststore Password")
        topicField = new JLabeledTextField("Topic")
        keyField = new JLabeledTextField("Key")
        avro = new JCheckBox("Enable avro data")
        closeAllBox = new JCheckBox("Close all Producers")
        closeBox = new JCheckBox("Close current Producer")

        mainPanel.add(samplerNameField)

        mainPanel.add(Box.createVerticalStrut(10))
        JSeparator separator = new JSeparator(SwingConstants.HORIZONTAL)
        mainPanel.add(separator)

        mainPanel.add(producerName)
        mainPanel.add(bootstrapField)
        mainPanel.add(keystorePathField)
        mainPanel.add(keystorePassField)  
        mainPanel.add(truststorePathField)
        mainPanel.add(truststorePassField) 
        mainPanel.add(topicField)
        mainPanel.add(keyField)

        JPanel headerPanel = new HorizontalPanel()
        headerPanel.add(new JLabel("Header:"))
        headerArea = new JTextArea(5, 50)
        headerArea.setLineWrap(true)
        headerPanel.add(new JScrollPane(headerArea))
        mainPanel.add(headerPanel)

        JPanel msgPanel = new HorizontalPanel()
        msgPanel.add(new JLabel("Message:"))
        messageArea = new JTextArea(15, 50)
        messageArea.setLineWrap(true)
        msgPanel.add(new JScrollPane(messageArea))
        mainPanel.add(msgPanel)

        mainPanel.add(Box.createVerticalStrut(10))
        JSeparator separator2 = new JSeparator(SwingConstants.HORIZONTAL)
        mainPanel.add(separator2)

        JPanel msgHelp3 = new VerticalPanel()
        msgHelp3.add(new JLabel("Enable avro data in base64"))
        mainPanel.add(msgHelp3)
        mainPanel.add(avro)

        mainPanel.add(Box.createVerticalStrut(10))

        JPanel msgHelp = new VerticalPanel()
        msgHelp.add(new JLabel("Close all producers for all threads"))
        mainPanel.add(msgHelp)
        mainPanel.add(closeAllBox)

        mainPanel.add(Box.createVerticalStrut(10))

        JPanel msgHelp2 = new VerticalPanel()
        msgHelp2.add(new JLabel("Close current producer for all threads"))
        mainPanel.add(msgHelp2)
        mainPanel.add(closeBox)

        add(mainPanel, BorderLayout.CENTER)
    }
}
