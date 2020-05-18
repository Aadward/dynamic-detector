package com.syh.example.dynamicdetector.checker.model;

import com.syh.example.dynamicdetector.common.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@lombok.Data
public class Alert {

    private long ruleId;

    private Data data;
}
