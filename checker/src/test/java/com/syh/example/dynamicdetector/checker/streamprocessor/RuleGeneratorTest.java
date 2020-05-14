package com.syh.example.dynamicdetector.checker.streamprocessor;

import com.google.common.collect.Lists;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import com.syh.example.dynamicdetector.checker.rule.model.RuleEvent;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.assertj.core.data.Index;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnit4.class)
public class RuleGeneratorTest {

    private OneInputStreamOperatorTestHarness<List<Rule>, RuleEvent> harness;
    private ProcessFunction<List<Rule>, RuleEvent> ruleEventGenerator;

    @Before
    public void init() throws Exception {
        ruleEventGenerator = new RuleEventGenerator();
        harness = ProcessFunctionTestHarnesses.forProcessFunction(ruleEventGenerator);
    }

    @Test
    public void add_one_rule() throws Exception {
        Rule input = newRule(1);
        RuleEvent expect = new RuleEvent(input, RuleEvent.EventType.NEW);

        harness.processElement(Lists.newArrayList(input), Long.MAX_VALUE);

        List<RuleEvent> ruleEvents = harness.extractOutputValues();
        assertThat(ruleEvents).containsOnly(expect);
    }

    @Test
    public void add_one_rule_andThen_update_it() throws Exception {
        Rule firstInput = newRule(1);
        harness.processElement(Lists.newArrayList(firstInput), Long.MAX_VALUE);

        Rule secondInput = firstInput.clone();
        secondInput.setUpdateTime(System.currentTimeMillis());
        harness.processElement(Lists.newArrayList(secondInput), Long.MAX_VALUE);

        List<RuleEvent> ruleEvents = harness.extractOutputValues();

        RuleEvent expect = new RuleEvent(secondInput, RuleEvent.EventType.UPDATE);
        assertThat(ruleEvents).contains(expect, Index.atIndex(1));
    }

    @Test
    public void add_one_rule_andThen_add_it_again() throws Exception {
        Rule firstInput = newRule(1);
        harness.processElement(Lists.newArrayList(firstInput), Long.MAX_VALUE);

        Rule secondInput = firstInput.clone();
        harness.processElement(Lists.newArrayList(secondInput), Long.MAX_VALUE);

        List<RuleEvent> ruleEvents = harness.extractOutputValues();

        RuleEvent expect = new RuleEvent(firstInput, RuleEvent.EventType.NEW);
        assertThat(ruleEvents).containsOnly(expect);
    }

    @Test
    public void add_one_rule_andThen_deleteIt() throws Exception {
        Rule firstInput = newRule(1);
        harness.processElement(Lists.newArrayList(firstInput), Long.MAX_VALUE);

        harness.processElement(Lists.newArrayList(), Long.MAX_VALUE);

        List<RuleEvent> ruleEvents = harness.extractOutputValues();

        RuleEvent expect = new RuleEvent(firstInput, RuleEvent.EventType.DELETE);
        assertThat(ruleEvents).contains(expect, Index.atIndex(1));
    }

    private Rule newRule(long ruleId) {
        return new Rule(ruleId, Lists.newArrayList(), null, System.currentTimeMillis());
    }

}
