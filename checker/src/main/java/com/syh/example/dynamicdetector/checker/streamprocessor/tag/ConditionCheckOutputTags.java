package com.syh.example.dynamicdetector.checker.streamprocessor.tag;

import com.syh.example.dynamicdetector.checker.model.RuleAppendedData;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.OutputTag;

public class ConditionCheckOutputTags {

    public static OutputTag<RuleAppendedData<Long>> noWindow =
            new OutputTag<>("no-window", TypeInformation.of(new TypeHint<RuleAppendedData<Long>>() {
            }));
    public static OutputTag<RuleAppendedData<Long>> withWindow =
            new OutputTag<>("with-window", TypeInformation.of(new TypeHint<RuleAppendedData<Long>>() {
            }));
}
