package hipravin.sampleskafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ClockTickEvent(
        @JsonProperty("ct") CloskType clockType,
        @JsonProperty("t") OffsetDateTime time,
        @JsonProperty("m") String message
) {
}
