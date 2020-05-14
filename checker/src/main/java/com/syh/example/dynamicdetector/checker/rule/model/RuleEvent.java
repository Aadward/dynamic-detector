package com.syh.example.dynamicdetector.checker.rule.model;

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
