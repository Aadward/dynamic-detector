package com.syh.example.dynamicdetector.checker.streamprocessor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.syh.example.dynamicdetector.checker.RuleEventBroadcastDescriptors;
import com.syh.example.dynamicdetector.checker.model.Data;
import com.syh.example.dynamicdetector.checker.model.RuleAppendedData;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import com.syh.example.dynamicdetector.checker.rule.model.Rule.Condition;
import com.syh.example.dynamicdetector.checker.rule.model.Rule.Window;
import com.syh.example.dynamicdetector.checker.rule.model.RuleEvent;
import com.syh.example.dynamicdetector.checker.streamprocessor.tag.ConditionCheckOutputTags;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class RuleConditionCheckProcessorTest {

    private BroadcastOperatorTestHarness<RuleAppendedData<Long>, RuleEvent, RuleAppendedData<Long>> harness;
    private BroadcastProcessFunction<RuleAppendedData<Long>, RuleEvent, RuleAppendedData<Long>> dataForkProcessor;

    @Before
    public void init() throws Exception {
        dataForkProcessor = new RuleEventConditionCheckProcessor();
        harness = ProcessFunctionTestHarnesses.forBroadcastProcessFunction(dataForkProcessor, RuleEventBroadcastDescriptors.descriptor);
    }

    @Test
    public void ruleWithWindow() throws Exception {
        Rule rule = new Rule(
                1L,
                Lists.newArrayList(new Condition("$.name", "shen.yuhang")),
                new Window(Lists.newArrayList("$.name"), 1, 2),
                System.currentTimeMillis());
        Data data = newData(1, "shen.yuhang");

        harness.processBroadcastElement(new RuleEvent(rule, RuleEvent.EventType.NEW), System.currentTimeMillis());
        harness.processElement(new RuleAppendedData<>(data, rule.getRuleId()), System.currentTimeMillis());

        assertThat(harness.getSideOutput(ConditionCheckOutputTags.noWindow))
                .isNullOrEmpty();

        assertThat(harness.getSideOutput(ConditionCheckOutputTags.withWindow))
                .size()
                .isEqualTo(1);
    }

    @Test
    public void ruleWithoutWindow() throws Exception {
        Rule rule = new Rule(
                1L,
                Lists.newArrayList(new Condition("$.name", "shen.yuhang")),
                null,
                System.currentTimeMillis());
        Data data = newData(1, "shen.yuhang");

        harness.processBroadcastElement(new RuleEvent(rule, RuleEvent.EventType.NEW), System.currentTimeMillis());
        harness.processElement(new RuleAppendedData<>(data, rule.getRuleId()), System.currentTimeMillis());

        assertThat(harness.getSideOutput(ConditionCheckOutputTags.withWindow))
                .isNullOrEmpty();

        assertThat(harness.getSideOutput(ConditionCheckOutputTags.noWindow))
                .size()
                .isEqualTo(1);
    }

    private Data newData(long eventTime, String name) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("name", name);
        map.put("eventTime", eventTime);
        return Data.fromMap(map);
    }
}
