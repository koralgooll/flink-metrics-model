package org.streammodels.models.sdsigmamodel;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


public class EventDecision extends ProcessFunction {

    @Override
    public void processElement(Object value, Context ctx, Collector out) throws Exception {
        Tuple4<Double, Double, Double, Double> stats = ((Tuple4<Double, Double, Double, Double>) value);
//        if (Math.abs(stats.f0 - stats.f2) / (stats.f1) > 3) out.collect(new Event("Error - unexpected fluctuations."));
        if (Math.abs(stats.f0 - stats.f2) / (stats.f1) > 3) {
            out.collect(new Tuple1<String>("Error - unexpected fluctuations."));
        }
    }
}