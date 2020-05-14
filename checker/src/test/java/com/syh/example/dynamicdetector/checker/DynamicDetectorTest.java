package com.syh.example.dynamicdetector.checker;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.syh.example.dynamicdetector.checker.model.Alert;
import com.syh.example.dynamicdetector.checker.model.Data;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class DynamicDetectorTest {

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @Before
    public void init() {
        CollectSink.values.clear();
    }

    @After
    public void destroy() {
        CollectSink.values.clear();
    }

    @Test
    public void testSingleRule() throws Exception {
        Data data = newData(1L, "shen.yuhang");
        Rule rule = new Rule(
                1L,
                Lists.newArrayList(new Rule.Condition("$.name", "shen.yuhang")),
                null,
                System.currentTimeMillis());

        RuleSource ruleSource = new RuleSource(Lists.newArrayList(rule));
        DataSource dataSource = new DataSource(Lists.newArrayList(data));


        DynamicDetector dynamicDetector = new DynamicDetector(ruleSource, dataSource, new CollectSink());
        dynamicDetector.run();

        Assertions.assertThat(CollectSink.values).containsOnly(new Alert(rule.getRuleId(), data));
    }

    @Test
    public void window_oneRule_intervalMatch() throws Exception {
        Data data1 = newData(1L, "shen.yuhang");
        Data data2 = newData(2L, "shen.yuhang");

        Rule rule = new Rule(
                1L,
                Lists.newArrayList(new Rule.Condition("$.name", "shen.yuhang")),
                new Rule.Window(Lists.newArrayList("$.name"), 1, 2),
                System.currentTimeMillis());

        RuleSource ruleSource = new RuleSource(Lists.newArrayList(rule));
        DataSource dataSource = new DataSource(Lists.newArrayList(data1, data2));


        DynamicDetector dynamicDetector = new DynamicDetector(ruleSource, dataSource, new CollectSink());
        dynamicDetector.run();

        Assertions.assertThat(CollectSink.values).containsOnly(new Alert(rule.getRuleId(), data2));
    }

    @Test
    public void window_oneRule_intervalNotMatch() throws Exception {
        Data data1 = newData(1L, "shen.yuhang");
        Data data2 = newData(2L + Time.minutes(1).toMilliseconds(), "shen.yuhang");

        Rule rule = new Rule(
                1L,
                Lists.newArrayList(new Rule.Condition("$.name", "shen.yuhang")),
                new Rule.Window(Lists.newArrayList("$.name"), 1, 2),
                System.currentTimeMillis());

        RuleSource ruleSource = new RuleSource(Lists.newArrayList(rule));
        DataSource dataSource = new DataSource(Lists.newArrayList(data1, data2));


        DynamicDetector dynamicDetector = new DynamicDetector(ruleSource, dataSource, new CollectSink());
        dynamicDetector.run();

        Assertions.assertThat(CollectSink.values).isEmpty();
    }


    @NoArgsConstructor
    @AllArgsConstructor
    @lombok.Data
    private static class RuleSource implements SourceFunction<List<Rule>>, Serializable {

        private List<Rule> rules;

        @Override
        public void run(SourceContext<List<Rule>> ctx) throws Exception {
            ctx.collect(rules);
        }

        @Override
        public void cancel() {

        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @lombok.Data
    private static class DataSource implements SourceFunction<Data>, Serializable {

        private List<Data> data;

        @Override
        public void run(SourceContext<Data> ctx) throws Exception {
            // make sure rule registered before data coming
            TimeUnit.MILLISECONDS.sleep(200);
            data.forEach(ctx::collect);
        }

        @Override
        public void cancel() {

        }
    }

    private static class CollectSink implements SinkFunction<Alert> {

        public static final List<Alert> values = new ArrayList<>();

        @Override
        public synchronized void invoke(Alert value) throws Exception {
            values.add(value);
        }
    }

    private Data newData(long eventTime, String name) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("name", name);
        map.put("eventTime", eventTime);
        return Data.fromMap(map);
    }
}
