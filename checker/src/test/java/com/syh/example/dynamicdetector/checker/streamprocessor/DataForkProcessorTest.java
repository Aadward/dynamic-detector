package com.syh.example.dynamicdetector.checker.streamprocessor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.syh.example.dynamicdetector.checker.RuleEventBroadcastDescriptors;
import com.syh.example.dynamicdetector.checker.model.Data;
import com.syh.example.dynamicdetector.checker.model.RuleAppendedData;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import com.syh.example.dynamicdetector.checker.rule.model.RuleEvent;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.util.BroadcastOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;

import static com.syh.example.dynamicdetector.checker.rule.model.Rule.Condition;
import static com.syh.example.dynamicdetector.checker.rule.model.Rule.Window;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class DataForkProcessorTest {

    private BroadcastOperatorTestHarness<Data, RuleEvent, RuleAppendedData<Long>> harness;
    private BroadcastProcessFunction<Data, RuleEvent, RuleAppendedData<Long>> dataForkProcessor;

    @Before
    public void init() throws Exception {
        dataForkProcessor = new DataForkProcessorEvent();
        harness = ProcessFunctionTestHarnesses.forBroadcastProcessFunction(dataForkProcessor, RuleEventBroadcastDescriptors.descriptor);
    }

    @Test
    public void forkData_oneDataWithTwoRule() throws Exception {
        Rule rule1 = newRule(1, "shen.yuhang");
        Rule rule2 = newRule(2, "shen.yuhang1");

        Data data = newData(1, "shen.yuhang");

        harness.processBroadcastElement(new RuleEvent(rule1, RuleEvent.EventType.NEW), System.currentTimeMillis());
        harness.processBroadcastElement(new RuleEvent(rule2, RuleEvent.EventType.NEW), System.currentTimeMillis());

        harness.processElement(data, System.currentTimeMillis());

        List<RuleAppendedData<Long>> result = harness.extractOutputValues();
        assertThat(result).containsOnlyElementsOf(Lists.newArrayList(
                new RuleAppendedData<>(data, 1L),
                new RuleAppendedData<>(data, 2L)
        ));
    }

    @Test
    public void forkData_twoRulesAndThenDeleteOneRule() throws Exception {
        Rule rule1 = newRule(1, "shen.yuhang");
        Rule rule2 = newRule(2, "shen.yuhang1");

        Data data1 = newData(1, "shen.yuhang");

        harness.processBroadcastElement(new RuleEvent(rule1, RuleEvent.EventType.NEW), System.currentTimeMillis());
        harness.processBroadcastElement(new RuleEvent(rule2, RuleEvent.EventType.NEW), System.currentTimeMillis());

        harness.processElement(data1, System.currentTimeMillis());

        List<RuleAppendedData<Long>> result = harness.extractOutputValues();
        assertThat(result).containsOnlyElementsOf(Lists.newArrayList(
                new RuleAppendedData<>(data1, 1L),
                new RuleAppendedData<>(data1, 2L)
        ));

        Data data2 = newData(2, "shen.yuhang");

        harness.processBroadcastElement(new RuleEvent(rule1, RuleEvent.EventType.DELETE), System.currentTimeMillis());
        harness.processElement(data2, System.currentTimeMillis());
        List<RuleAppendedData<Long>> afterDelete = harness.extractOutputValues();
        assertThat(afterDelete).containsOnlyElementsOf(Lists.newArrayList(
                new RuleAppendedData<>(data1, 1L),
                new RuleAppendedData<>(data1, 2L),
                new RuleAppendedData<>(data2, 2L)
        ));
    }

    private Data newData(long eventTime, String name) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("name", name);
        map.put("eventTime", eventTime);
        return Data.fromMap(map);
    }

    private Rule newRule(int ruleId, String expectName) {
        return new Rule(ruleId,
                Lists.newArrayList(new Condition("$.name", expectName)),
                new Window(Lists.newArrayList("$.name"), 1, 10),
                System.currentTimeMillis());
    }
}
