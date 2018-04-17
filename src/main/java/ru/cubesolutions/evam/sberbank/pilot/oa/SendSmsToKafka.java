package ru.cubesolutions.evam.sberbank.pilot.oa;

import com.intellica.evam.sdk.outputaction.AbstractOutputAction;
import com.intellica.evam.sdk.outputaction.IOMParameter;
import com.intellica.evam.sdk.outputaction.OutputActionContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by Garya on 27.11.2017.
 */
public class SendSmsToKafka extends AbstractOutputAction {

    private final static Logger log = Logger.getLogger(SendSmsToKafka.class);

    private Producer producer;
    private String topic;

    @Override
    public synchronized void init() {
        isInited = false;
        try (InputStream input = new FileInputStream("./conf/sberbank-pilot.properties")) {
            Properties props = new Properties();
            props.load(input);
            Properties kafkaProps = new Properties();
            kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("kafka-sms-url-with-port"));
            kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.getProperty("kafka-sms-key-serializer-class"));
            kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.getProperty("kafka-sms-value-serializer-class"));
            this.producer = new KafkaProducer<String, String>(kafkaProps);
            this.topic = props.getProperty("kafka-sms-topic");
        } catch (Exception e) {
            log.error("failed to init SendSmsToKafka action", e);
            isInited = false;
            return;
        }
        isInited = true;
    }

    public int execute(OutputActionContext outputActionContext) throws Exception {
        String notificationId = (String) outputActionContext.getParameter("notification_id");
        log.debug("notificationId is " + notificationId);
        String campaignId = (String) outputActionContext.getParameter("campaign_id");
        log.debug("campaignId is " + campaignId);
        String clientId = (String) outputActionContext.getParameter("client_id");
        log.debug("clientId is " + clientId);
        String phoneNumber = (String) outputActionContext.getParameter("phone_number");
        log.debug("phoneNumber is " + phoneNumber);
        String text = (String) outputActionContext.getParameter("text");
        log.debug("text is " + text);
        String testMode = (String) outputActionContext.getParameter("test_mode");
        log.debug("testMode is " + testMode);

        String message = formatSmsJson(notificationId, campaignId, clientId, phoneNumber, text, testMode);
        log.debug("json sms: " + message);
        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(this.topic, message);
        log.debug("message is sending..");
        this.producer.send(rec);
        this.producer.flush();
        log.debug("message is sent successfully");
        return 0;
    }

    private static String formatSmsJson(String notificationId,
                                        String campaignId,
                                        String clientId,
                                        String phoneNumber,
                                        String text,
                                        String testMode) {

        return String.format(smsJsonStructure(),
                notificationId,
                campaignId,
                clientId,
                notificationId,
                phoneNumber,
                text,
                testMode);
    }

    private static String smsJsonStructure() {
        return "{\"notification_id\":%s," +
                "\"campaign_id\":%s," +
                "\"client_id\":%s," +
                "\"transaction_id\":%s," +
                "\"phone_number\":\"%s\"," +
                "\"text\":\"%s\"," +
                "\"test_mode\":%s" +
                "}";
    }

    protected ArrayList<IOMParameter> getParameters() {
        ArrayList<IOMParameter> params = new ArrayList<>();
        params.add(new IOMParameter("notification_id", "notification id"));
        params.add(new IOMParameter("campaign_id", "campaign id"));
        params.add(new IOMParameter("client_id", "client id"));
        params.add(new IOMParameter("phone_number", "phone number"));
        params.add(new IOMParameter("text", "text"));
        params.add(new IOMParameter("test_mode", "test mode"));
        return params;
    }

    public String getVersion() {
        return "1.0";
    }
}
