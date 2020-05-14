package com.syh.example.dynamicdetector.checker.rule;

import com.syh.example.dynamicdetector.checker.model.Data;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;

import java.util.Objects;

public class RuleParser {

    public static boolean matchConditions(Rule rule, Data data) {
        return rule.getConditions()
                .stream()
                .allMatch(condition -> Objects.equals(data.get(condition.getFieldName()), condition.getExpect()));
    }
}
