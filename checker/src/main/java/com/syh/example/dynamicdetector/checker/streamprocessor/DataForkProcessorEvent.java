package com.syh.example.dynamicdetector.checker.streamprocessor;

import com.syh.example.dynamicdetector.checker.RuleEventBroadcastDescriptors;
import com.syh.example.dynamicdetector.checker.model.RuleAppendedData;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import com.syh.example.dynamicdetector.common.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.util.Collector;

@Slf4j
public class DataForkProcessorEvent extends RuleEventConnectedProcessor<Data, RuleAppendedData<Long>> {

    @Override
    public void processElement(Data value, ReadOnlyContext ctx, Collector<RuleAppendedData<Long>> out) throws Exception {
        ReadOnlyBroadcastState<Long, Rule> state = ctx.getBroadcastState(RuleEventBroadcastDescriptors.descriptor);

        state.immutableEntries().forEach(entry -> {
            Rule rule = entry.getValue();
            out.collect(new RuleAppendedData<>(value, rule.getRuleId()));
        });
    }
}
