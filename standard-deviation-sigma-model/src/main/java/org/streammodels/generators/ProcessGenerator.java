package org.streammodels.generators;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

import java.util.ArrayList;

@Experimental
public abstract class ProcessGenerator<T> implements DataGenerator <T> {
    // TODO : Refactor to use math3 library instead of RandomSampler.
    protected transient RandomSampler<Integer> sampler;
    protected transient ArrayList<Integer> values;

    @Override
    public void open(String name, FunctionInitializationContext context, RuntimeContext runtimeContext) throws Exception {
        this.sampler = new BernoulliSampler<>(0.5);
        this.values = new ArrayList(){{add(1);}};
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    public static ProcessGenerator<Integer> bernoulliProcessGenerator(double fraction) {
        return new ProcessGenerator<Integer>() {
            @Override
            public Integer next() {
                if (sampler.sample(values.iterator()).hasNext()) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };
    }
}
