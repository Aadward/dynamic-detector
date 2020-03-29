package com.syh.example.rule;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.syh.example.model.Data;
import com.syh.example.rule.model.Rule;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.syh.example.rule.model.Rule.Condition;

@RunWith(JUnit4.class)
public class RuleParserTest {

    @Test
    public void matchConditions_yes() throws Exception {
        Condition condition = new Condition("$.name", "shen.yuhang");
        Rule rule = new Rule(1, Lists.newArrayList(condition), null, System.currentTimeMillis());
        Data data = Data.fromMap(ImmutableMap.of("name", "shen.yuhang"));

        Assertions.assertThat(RuleParser.matchConditions(rule, data)).isTrue();
    }

    @Test
    public void matchConditions_notMatch() throws Exception {
        Condition condition = new Condition("$.name", "shen.yuhang1");
        Rule rule = new Rule(1, Lists.newArrayList(condition), null, System.currentTimeMillis());
        Data data = Data.fromMap(ImmutableMap.of("name", "shen.yuhang"));

        Assertions.assertThat(RuleParser.matchConditions(rule, data)).isFalse();
    }
}
