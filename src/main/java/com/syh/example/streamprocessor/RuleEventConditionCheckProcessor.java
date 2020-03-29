package com.syh.example.streamprocessor;

import com.syh.example.RuleEventBroadcastDescriptors;
import com.syh.example.model.RuleAppendedData;
import com.syh.example.rule.RuleParser;
import com.syh.example.rule.model.Rule;
import com.syh.example.streamprocessor.tag.ConditionCheckOutputTags;
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
