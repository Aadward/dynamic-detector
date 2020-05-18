package com.syh.example.dynamicdetector.checker;

import com.syh.example.dynamicdetector.checker.model.*;
import com.syh.example.dynamicdetector.checker.rule.model.Rule;
import com.syh.example.dynamicdetector.checker.rule.model.RuleEvent;
import com.syh.example.dynamicdetector.checker.streamprocessor.*;
import com.syh.example.dynamicdetector.checker.streamprocessor.tag.ConditionCheckOutputTags;
import com.syh.example.dynamicdetector.common.Data;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.List;

public class DynamicDetector {

    SourceFunction<List<Rule>> ruleSource;

    SourceFunction<Data> dataSource;

    SinkFunction<Alert> sink;

    public DynamicDetector(SourceFunction<List<Rule>> ruleSource, SourceFunction<Data> dataSource, SinkFunction<Alert> sink) {
        this.ruleSource = ruleSource;
        this.dataSource = dataSource;
        this.sink = sink;
    }

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        BroadcastStream<RuleEvent> ruleEventDataStream =
                env.addSource(ruleSource)
                    .process(new RuleEventGenerator())
                    .broadcast(RuleEventBroadcastDescriptors.descriptor);

        SingleOutputStreamOperator<RuleAppendedData<Long>> matchedRuleStream = env.addSource(dataSource)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Data>() {
                    @Override
                    public long extractAscendingTimestamp(Data element) {
                        return ((Number) element.get("$.eventTime")).longValue();
                    }
                })
                .connect(ruleEventDataStream)
                .process(new DataForkProcessorEvent())
                .connect(ruleEventDataStream)
                .process(new RuleEventConditionCheckProcessor());

        matchedRuleStream
                .getSideOutput(ConditionCheckOutputTags.noWindow)
                .connect(ruleEventDataStream)
                .process(new RuleWithoutWindowAlertGenerator())
                .addSink(sink);

        matchedRuleStream
                .getSideOutput(ConditionCheckOutputTags.withWindow)
                .connect(ruleEventDataStream)
                .process(new AppendKeyProcessor())
                .keyBy(new KeySelector<Keyed<Key, RuleAppendedData<Long>>, Key>() {
                    @Override
                    public Key getKey(Keyed<Key, RuleAppendedData<Long>> value) throws Exception {
                        return value.getKey();
                    }
                })
                .connect(ruleEventDataStream)
                .process(new WindowProcessor())
                .addSink(sink);

        env.execute("Dynamic Detector Job");
    }
}
