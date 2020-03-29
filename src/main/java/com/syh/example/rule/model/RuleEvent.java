package com.syh.example.rule.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class RuleEvent {

    private Rule rule;

    private EventType eventType;

    public enum EventType {
        NEW,
        UPDATE,
        DELETE
    }
}
