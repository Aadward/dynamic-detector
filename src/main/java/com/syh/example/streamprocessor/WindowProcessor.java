package com.syh.example.streamprocessor;

import com.google.common.collect.Lists;
import com.syh.example.model.*;
import com.syh.example.rule.model.Rule;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WindowProcessor extends KeyedRuleEventConnectedProcessor<Key, Keyed<Key, RuleAppendedData<Long>>, Alert> {

    private MapStateDescriptor<Long, Set<Data>> windowStateDescriptor =
            new MapStateDescriptor<Long, Set<Data>>("windowDescriptor", Types.LONG,
                    TypeInformation.of(new TypeHint<Set<Data>>() {
                    }));

    private MapState<Long, Set<Data>> windowState;

    @Override
    public void open(Configuration parameters) throws Exception {
        windowState = getRuntimeContext().getMapState(windowStateDescriptor);
    }

    @Override
    public void processElement(Keyed<Key, RuleAppendedData<Long>> value, ReadOnlyContext ctx, Collector<Alert> out) throws Exception {
        RuleAppendedData<Long> ruleAppendedData = value.getValue();
        Rule rule = getRule(ctx, ruleAppendedData.getRuleId());
        if (rule == null) {
            return;
        }

        long elementTime = ctx.timestamp();
        Set<Data> set = windowState.get(elementTime);
        if (set == null) {
            set = new HashSet<>();
        }
        set.add(value.getValue().getData());
        windowState.put(elementTime, set);

        ctx.timerService().registerEventTimeTimer(elementTime);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        Key key = ctx.getCurrentKey();
        Rule rule = getRule(ctx, key.getRuleId());
        if (rule == null) {
            return;
        }
        long startTime = timestamp - Time.minutes(rule.getWindow().getWindowMinute()).toMilliseconds();

        List<Data> inWindow = Lists.newArrayList();
        for (Map.Entry<Long, Set<Data>> entry : windowState.entries()) {
            long t = entry.getKey();
            if (t >= startTime && t <= timestamp) {
                inWindow.addAll(entry.getValue());
            }
        }

        if (inWindow.size() >= rule.getWindow().getLimit()) {
            out.collect(new Alert(rule.getRuleId(), inWindow.get(inWindow.size() - 1)));
        }
    }
}
