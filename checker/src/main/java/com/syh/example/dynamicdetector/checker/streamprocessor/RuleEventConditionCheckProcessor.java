package com.syh.example.dynamicdetector.checker.streamprocessor;

import com.syh.example.dynamicdetector.checker.RuleEventBroadcastDescriptors;
import com.syh.example.dynamicdetector.checker.model.RuleAppendedData;
import com.syh.example.dynamicdetector.checker.rule.RuleParser;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import com.syh.example.dynamicdetector.checker.streamprocessor.tag.ConditionCheckOutputTags;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.util.Collector;

public class RuleEventConditionCheckProcessor extends RuleEventConnectedProcessor<RuleAppendedData<Long>, RuleAppendedData<Long>> {

    @Override
    public void processElement(RuleAppendedData<Long> value, ReadOnlyContext ctx, Collector<RuleAppendedData<Long>> out) throws Exception {
        ReadOnlyBroadcastState<Long, Rule> state = ctx.getBroadcastState(RuleEventBroadcastDescriptors.descriptor);

        Rule rule = state.get(value.getRuleId());
        if (rule == null) {
            return;
        }

        if (RuleParser.matchConditions(rule, value.getData())) {
            if (rule.getWindow() != null) {
                ctx.output(ConditionCheckOutputTags.withWindow, value);
            } else {
                ctx.output(ConditionCheckOutputTags.noWindow, value);
            }
        }
    }
}
