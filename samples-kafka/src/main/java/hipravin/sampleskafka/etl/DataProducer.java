package hipravin.sampleskafka.etl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Component
public class DataProducer {
    private static final Logger log = LoggerFactory.getLogger(DataProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public DataProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Autowired
    public void autowired() {
        log.info("The moment autowired is called");
    }

    @EventListener(ApplicationReadyEvent.class)
    public void clockSecondsIndefinitely() {
        log.info("The moment ApplicationReadyEvent is received");

        final var delayMillis = 0;
        final var rateMillis = 1000;
        var scheduler = Executors.newScheduledThreadPool(4);

        scheduler.scheduleAtFixedRate(() -> {
            var now = LocalDateTime.now();

            var sr = kafkaTemplate.send("clock-long-topic", String.valueOf(now.getSecond()), now.format(DateTimeFormatter.ISO_TIME) + ", " + Thread.currentThread().getName());
            try {
                log.info("Send result: {}", sr.get().toString());
            } catch (InterruptedException | ExecutionException e) {
                log.error(e.getMessage(), e);
            }
        }, delayMillis, rateMillis, TimeUnit.MILLISECONDS);

        try {
            Thread.sleep(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        scheduler.shutdown();
    }
}
