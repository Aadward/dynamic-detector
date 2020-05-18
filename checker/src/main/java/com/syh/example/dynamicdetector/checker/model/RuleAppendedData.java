package com.syh.example.dynamicdetector.checker.model;

import com.syh.example.dynamicdetector.common.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@lombok.Data
@AllArgsConstructor
@NoArgsConstructor
public class RuleAppendedData<ID> {

    private Data data;

    private ID ruleId;
}
