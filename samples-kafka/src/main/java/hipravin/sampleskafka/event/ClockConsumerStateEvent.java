package hipravin.sampleskafka.event;

import org.springframework.context.ApplicationEvent;

import java.time.Clock;

public class ClockConsumerStateEvent extends ApplicationEvent {
    public enum State {
        STARTED,
        FINISHED
    }

    private final State state;

    ClockConsumerStateEvent(Object source, State state) {
        super(source);
        this.state = state;
    }

    public static ClockConsumerStateEvent started(Object source) {
        return new ClockConsumerStateEvent(source, State.STARTED);
    }

    public static ClockConsumerStateEvent finished(Object source) {
        return new ClockConsumerStateEvent(source, State.FINISHED);
    }


    public ClockConsumerStateEvent(Object source, Clock clock, State state) {
        super(source, clock);
        this.state = state;
    }

    public State getState() {
        return state;
    }
}
