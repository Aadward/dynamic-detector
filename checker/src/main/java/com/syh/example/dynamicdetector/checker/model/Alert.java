package com.syh.example.dynamicdetector.checker.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;


@AllArgsConstructor
@NoArgsConstructor
@lombok.Data
public class Alert {

    private long ruleId;

    private Data data;
}
