package com.syh.example.streamprocessor;

import com.google.common.collect.Lists;
import com.syh.example.rule.model.Rule;
import com.syh.example.rule.model.RuleEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RuleEventGenerator extends ProcessFunction<List<Rule>, RuleEvent> implements ListCheckpointed<Rule> {

    private List<Rule> currentRules;

    @Override
    public void open(Configuration parameters) throws Exception {
        if (currentRules == null) {
            currentRules = Lists.newArrayList();
        }
    }

    @Override
    public List<Rule> snapshotState(long checkpointId, long timestamp) throws Exception {
        return currentRules;
    }

    @Override
    public void restoreState(List<Rule> state) throws Exception {
        this.currentRules = state;
    }

    @Override
    public void processElement(List<Rule> rules, Context ctx, Collector<RuleEvent> out) throws Exception {
        Map<Long, Rule> existRuleMap = currentRules
                .stream()
                .collect(Collectors.toMap(Rule::getRuleId, Function.identity()));

        List<RuleEvent> events = Lists.newArrayList();

        for (Rule rule : rules) {
            if (!existRuleMap.containsKey(rule.getRuleId())) {
                events.add(new RuleEvent(rule, RuleEvent.EventType.NEW));
            } else {
                // exist
                Rule exist = existRuleMap.get(rule.getRuleId());
                if (!exist.equals(rule)) {
                    events.add(new RuleEvent(rule, RuleEvent.EventType.UPDATE));
                }
            }
            existRuleMap.remove(rule.getRuleId());
        }
        existRuleMap.values().forEach(rule -> out.collect(new RuleEvent(rule, RuleEvent.EventType.DELETE)));

        currentRules = rules;
        events.forEach(out::collect);
    }
}
