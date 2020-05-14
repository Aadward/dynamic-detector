package com.syh.example.dynamicdetector.checker.rule.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Rule implements Serializable, Cloneable {

    private long ruleId;

    private List<Condition> conditions;

    private Window window;

    private long updateTime;

    // TODO:
    //  Problem is: condition and window are reference, not cloned
    @Override
    public Rule clone() throws CloneNotSupportedException {
        return new Rule(ruleId, conditions, window, updateTime);
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Condition implements Serializable {
        private String fieldName;
        private Object expect;
    }

    @AllArgsConstructor
    @NoArgsConstructor
    @Data
    public static class Window implements Serializable {

        private List<String> groupingBy;

        private int windowMinute;

        private long limit;
    }
}
