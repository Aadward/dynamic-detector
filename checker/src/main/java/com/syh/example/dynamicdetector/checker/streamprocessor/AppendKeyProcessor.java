package com.syh.example.dynamicdetector.checker.streamprocessor;

import com.google.common.collect.Maps;
import com.syh.example.dynamicdetector.checker.model.Key;
import com.syh.example.dynamicdetector.checker.model.Keyed;
import com.syh.example.dynamicdetector.checker.model.RuleAppendedData;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import org.apache.flink.util.Collector;

import java.util.Map;

public class AppendKeyProcessor extends RuleEventConnectedProcessor<RuleAppendedData<Long>, Keyed<Key, RuleAppendedData<Long>>> {

    @Override
    public void processElement(RuleAppendedData<Long> value, ReadOnlyContext ctx, Collector<Keyed<Key, RuleAppendedData<Long>>> out) throws Exception {
        Rule rule = getRule(ctx, value.getRuleId());
        if (rule == null) {
            return;
        }

        Map<String, Object> groupingValues = Maps.newHashMap();
        rule.getWindow()
                .getGroupingBy()
                .forEach(fieldName -> groupingValues.put(fieldName, value.getData().get(fieldName)));

        out.collect(new Keyed<>(new Key(rule.getRuleId(), groupingValues), value));
    }
}
