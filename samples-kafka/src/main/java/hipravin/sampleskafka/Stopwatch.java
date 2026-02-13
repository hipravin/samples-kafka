package hipravin.sampleskafka;

import hipravin.sampleskafka.event.ClockConsumerStateEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class Stopwatch {
    private static final Logger log = LoggerFactory.getLogger(Stopwatch.class);

    private final AtomicReference<Long> processingStarted = new AtomicReference<>(null);

    @Async
    @EventListener(ClockConsumerStateEvent.class)
    public void consumerStarted(ClockConsumerStateEvent event) {
        switch(event.getState()) {
            case STARTED -> {
                processingStarted.set(System.nanoTime());
            }
            case FINISHED -> {
                if(processingStarted.get() != null) {
                    var elapsed = Duration.ofNanos(System.nanoTime() - processingStarted.get());
                    log.info("Processing took: {} ms", elapsed.toMillis());
                } else {
                    log.warn("Processing finished, but start instant not catched.");
                }
            }
        }
    }
}
