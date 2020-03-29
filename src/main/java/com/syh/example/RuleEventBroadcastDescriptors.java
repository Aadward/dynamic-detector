package com.syh.example;

import com.syh.example.rule.model.Rule;
import org.apache.flink.api.common.state.MapStateDescriptor;

public class RuleEventBroadcastDescriptors {

    public static MapStateDescriptor<Long, Rule> descriptor = new MapStateDescriptor<Long, Rule>(
            "ruleBroadcastState", Long.class, Rule.class
    );
}
