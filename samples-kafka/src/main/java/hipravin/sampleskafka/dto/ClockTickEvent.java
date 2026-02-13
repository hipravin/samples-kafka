package hipravin.sampleskafka.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;

@JsonIgnoreProperties(ignoreUnknown = true)
public record ClockTickEvent(
        @JsonProperty("ct") CloskType clockType,
        @JsonProperty("t") @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") OffsetDateTime time,
        @JsonProperty("m") String message

) {
}
