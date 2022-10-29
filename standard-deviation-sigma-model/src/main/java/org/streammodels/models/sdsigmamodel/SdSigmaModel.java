/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streammodels.models.sdsigmamodel;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.streammodels.generators.ProcessGenerator;


/**
 * The Ride Cleansing exercise from the Flink training.
 *
 * <p>The task of this exercise is to filter a data stream of taxi ride records to keep only rides
 * that both start and end within New York City. The resulting stream should be printed.
 */
public class SdSigmaModel {

    private final DataGeneratorSource<Integer> source;
    private final SinkFunction<Integer> sink;

    private final RandomSampler<Integer> sampler;

    /**
     * Creates a job using the source and sink provided.
     */
    public SdSigmaModel(DataGeneratorSource<Integer> source,
                        SinkFunction<Integer> sink,
                        RandomSampler<Integer> sampler) {
        this.source = source;
        this.sink = sink;
        this.sampler = sampler;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        RandomSampler<Integer> sampler = new BernoulliSampler<>(0.5);
//        SdSigmaModel job =
//                new SdSigmaModel(
//                        new DataGeneratorSource<>(RandomGenerator.intGenerator(1, 100)),
//                        new PrintSinkFunction<>(), sampler);

        SdSigmaModel job =
                new SdSigmaModel(
                        new DataGeneratorSource<>(ProcessGenerator.bernoulliProcessGenerator(0.5)),
                        new PrintSinkFunction<>(), sampler);

        job.execute();
    }

    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set up the pipeline
        env.addSource(source, TypeInformation.of(Integer.class)).filter(new Filter99()).addSink(sink);

        return env.execute("Taxi Ride Cleansing");
    }

    /**
     * Keep only those rides and both start and end in NYC.
     */
    public static class Filter99 implements FilterFunction<Integer> {

        @Override
        public boolean filter(Integer value) throws Exception {
            return value < 98;
        }
    }
}
