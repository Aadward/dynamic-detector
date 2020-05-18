package com.syh.example.dynamicdetector.checker;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import com.syh.example.dynamicdetector.common.Data;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        DynamicDetector detector = new DynamicDetector(ruleSource, new SourceFunction<Data>() {
            @Override
            public void run(SourceContext<Data> ctx) throws Exception {
                TimeUnit.MILLISECONDS.sleep(500);
                Data data1 = newData(1L, "shen.yuhang");
                Data data2 = newData(2L + Time.minutes(1).toMilliseconds(), "shen.yuhang");
                Data data3 = newData(3L + Time.minutes(2).toMilliseconds(), "shen.yuhang");
                ctx.collect(data1);
                ctx.collect(data2);
                ctx.collect(data3);

                TimeUnit.DAYS.sleep(1);
            }

            @Override
            public void cancel() {

            }
        }, new PrintSinkFunction<>());

        detector.run();
    }

    private static final Rule RULE = new Rule(
            1L,
            Lists.newArrayList(new Rule.Condition("$.name", "shen.yuhang")),
            new Rule.Window(Lists.newArrayList("$.name"), 200, 2),
            System.currentTimeMillis());

    private static SourceFunction<List<Rule>> ruleSource = new SourceFunction<List<Rule>>() {
        @Override
        public void run(SourceContext<List<Rule>> ctx) throws Exception {
            ctx.collect(Collections.singletonList(RULE));
        }

        @Override
        public void cancel() {

        }
    };

    private static Data newData(long eventTime, String name) {
        Map<String, Object> map = Maps.newHashMap();
        map.put("name", name);
        map.put("eventTime", eventTime);
        return Data.fromMap(map);
    }
}
