package hipravin.sampleskafka;

import hipravin.sampleskafka.dto.ClockTickEvent;
import hipravin.sampleskafka.event.ClockConsumerStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

@Component
public class KafkaListenerExample {
    private static final Logger log = LoggerFactory.getLogger(KafkaListenerExample.class);


    private final long messageCount;
    private final AtomicLong shortTickCount = new AtomicLong(0);
    private final ApplicationEventPublisher eventPublisher;

    public KafkaListenerExample(@Value("${application.clock-producer.message-count}") long messageCount,
                                ApplicationEventPublisher eventPublisher) {
        this.messageCount = messageCount;
        this.eventPublisher = eventPublisher;
    }

    @RetryableTopic(attempts = "5", numPartitions = "2",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            backOff = @BackOff(delay = 1000, maxDelay = 25000, multiplier = 1.5))
    @KafkaListener(topics = {"clock-long-topic"}, groupId = "group1")
    void listenerG1(@Payload ClockTickEvent tickEvent,
                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                    @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Received [{}] from group1, partition-{} with offset-{}, topic {}",
                tickEvent, partition, offset, topic);

//            log.warn("Random exception [{}] from group1, partition-{} with offset-{}, topic {}",
//                    tickEvent, partition, offset, topic);

//            throw new RuntimeException("Random failure");
    }

    @RetryableTopic(attempts = "5", numPartitions = "2",
//            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            backOff = @BackOff(delay = 1000, maxDelay = 25000, multiplier = 1.5))
    @KafkaListener(topics = {"clock-short-topic"}, groupId = "group2")
    void listenerG2(@Payload ClockTickEvent tickEvent,
                    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                    @Header(KafkaHeaders.OFFSET) long offset) {
        long count = shortTickCount.incrementAndGet();

        if(count == 1) {
            eventPublisher.publishEvent(ClockConsumerStateEvent.started(this));
        }
        if(count == messageCount) {
            eventPublisher.publishEvent(ClockConsumerStateEvent.finished(this));
        }

        if (count % 1_000 == 0) {
            log.info("Short tick message count:  {}", count);
        }
    }

    @DltHandler
    public void handleDlt(ClockTickEvent clockTickEvent) {
        log.error("DLT handlerxx {}", clockTickEvent);
    }
}