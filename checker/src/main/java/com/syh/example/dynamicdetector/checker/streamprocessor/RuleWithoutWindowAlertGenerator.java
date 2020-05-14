package com.syh.example.dynamicdetector.checker.streamprocessor;

import com.syh.example.dynamicdetector.checker.RuleEventBroadcastDescriptors;
import com.syh.example.dynamicdetector.checker.model.Alert;
import com.syh.example.dynamicdetector.checker.model.RuleAppendedData;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.util.Collector;

public class RuleWithoutWindowAlertGenerator extends RuleEventConnectedProcessor<RuleAppendedData<Long>, Alert> {

    @Override
    public void processElement(RuleAppendedData<Long> value, ReadOnlyContext ctx, Collector<Alert> out) throws Exception {
        ReadOnlyBroadcastState<Long, Rule> state = ctx.getBroadcastState(RuleEventBroadcastDescriptors.descriptor);

        Rule rule = state.get(value.getRuleId());
        if (rule == null) {
            return;
        }

        out.collect(new Alert(rule.getRuleId(), value.getData()));
    }
}
