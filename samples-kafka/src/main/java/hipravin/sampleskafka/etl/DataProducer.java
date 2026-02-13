package hipravin.sampleskafka.etl;

import hipravin.sampleskafka.dto.ClockTickEvent;
import hipravin.sampleskafka.dto.CloskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

@Component
public class DataProducer implements InitializingBean, DisposableBean {
    private static final Logger log = LoggerFactory.getLogger(DataProducer.class);

    private final long messageCount;

    private final KafkaTemplate<String, ClockTickEvent> kafkaClockTemplate;
    private Clock clock = Clock.systemDefaultZone();

    public DataProducer(@Value("${application.clock-producer.message-count:0}") long messageCount,
                        KafkaTemplate<String, ClockTickEvent> kafkaTemplate) {
        this.messageCount = messageCount;
        this.kafkaClockTemplate = kafkaTemplate;
    }

    @Autowired
    public void autowired() {
        log.info("The moment autowired is called");
    }

    @Async
    @EventListener(value = ApplicationReadyEvent.class,
            condition = "@environment.getProperty('application.clock-producer.enabled') == 'true'")
    public void tickSeconds() {
        log.info("The moment ApplicationReadyEvent is received, thread: {}", Thread.currentThread());

        final var delayMillis = 0;
        final var rateMillis = 1000;
        var scheduler = Executors.newScheduledThreadPool(4);

        scheduler.scheduleAtFixedRate(() -> {
            var now = OffsetDateTime.now();
            try {
                var sr = kafkaClockTemplate.send("clock-long-topic", String.valueOf(now.getSecond()),
                        new ClockTickEvent(CloskType.LONG, now, UUID.randomUUID().toString()));
                sr.thenAccept(r -> {
                    log.info("Send result: {}", r);
                }).exceptionally(e -> {
                    log.error(e.getMessage(), e);
                    return null;
                });
            } catch (RuntimeException e) {
                log.error("Failed to send: {}", e.getMessage(), e);
            }
        }, delayMillis, rateMillis, TimeUnit.MILLISECONDS);

        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        scheduler.shutdown();
    }

    @Async
    @EventListener(value = ApplicationReadyEvent.class,
            condition = "@environment.getProperty('application.clock-producer.enabled') == 'true'")
    public void tickMillis() {
        long start = System.nanoTime();
        log.info("The moment ApplicationReadyEvent is received, thread: {}", Thread.currentThread());
        log.info("Producing {} messages", messageCount);

        LongStream.range(0, messageCount)
//                .parallel()
                .forEach(i -> {
                    sendTick(OffsetDateTime.now(clock), UUID.randomUUID());
                });
        log.info("Publishing {} events has finished in {} ms", messageCount, Duration.ofNanos(System.nanoTime() - start).toMillis());
    }

    private void sendTick(OffsetDateTime time, UUID uuid) {

        try {
            var sr = kafkaClockTemplate.send("clock-short-topic", String.valueOf(time.getSecond()),
                    new ClockTickEvent(CloskType.SHORT, time, uuid.toString()));
            sr.exceptionally(e -> {
                log.error(e.getMessage(), e);
                return null;
            });
        } catch (RuntimeException e) {
            log.error("Failed to send: {}", e.getMessage(), e);
        }
    }

    @Override
    public void destroy() throws Exception {
        log.info("The moment destroy is called");
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        log.info("The moment afterPropertiesSet is called");
    }
}
