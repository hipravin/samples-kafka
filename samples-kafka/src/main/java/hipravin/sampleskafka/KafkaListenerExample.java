package hipravin.sampleskafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class KafkaListenerExample {
    private static final Logger log = LoggerFactory.getLogger(KafkaListenerExample.class);

    @KafkaListener(topics = {"clock-short-topic","clock-long-topic"}, groupId = "group1")
    void listenerG1(@Payload String data,
                  @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                  @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Received [{}] from group1, partition-{} with offset-{}, topic {}",
                data, partition, offset, topic);
    }

    @KafkaListener(topics = {"clock-short-topic","clock-long-topic"}, groupId = "group2")
    void listenerG2(@Payload String data,
                  @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                  @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Received [{}] from group2, partition-{} with offset-{}, topic {}",
                data, partition, offset, topic);
    }

}