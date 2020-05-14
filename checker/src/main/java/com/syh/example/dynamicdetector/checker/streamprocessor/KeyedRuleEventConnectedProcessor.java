package com.syh.example.dynamicdetector.checker.streamprocessor;

import com.syh.example.dynamicdetector.checker.RuleEventBroadcastDescriptors;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import com.syh.example.dynamicdetector.checker.rule.model.RuleEvent;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public abstract class KeyedRuleEventConnectedProcessor<KEY, IN, OUT> extends KeyedBroadcastProcessFunction<KEY, IN, RuleEvent, OUT> {

    @Override
    public void processBroadcastElement(RuleEvent value, Context ctx, Collector<OUT> out) throws Exception {
        BroadcastState<Long, Rule> state = ctx.getBroadcastState(RuleEventBroadcastDescriptors.descriptor);

        if (value.getEventType() == RuleEvent.EventType.NEW || value.getEventType() == RuleEvent.EventType.UPDATE) {
            state.put(value.getRule().getRuleId(), value.getRule());
        } else if (value.getEventType() == RuleEvent.EventType.DELETE) {
            state.remove(value.getRule().getRuleId());
        }

        onRuleChanged(value.getRule(), value.getEventType());
    }

    protected Rule getRule(ReadOnlyContext ctx, long ruleId) throws Exception {
        return ctx.getBroadcastState(RuleEventBroadcastDescriptors.descriptor).get(ruleId);
    }

    protected void onRuleChanged(Rule rule, RuleEvent.EventType eventType) {
        // default for do nothing
    }
}
