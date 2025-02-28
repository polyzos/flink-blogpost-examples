package io.ipolyzos.triggers;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserEvent {
    private String userId;
    private String eventType;
    private long timestamp;
}
