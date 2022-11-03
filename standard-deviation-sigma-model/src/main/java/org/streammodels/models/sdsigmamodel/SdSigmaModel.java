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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.streammodels.generators.ProcessGenerator;
import org.streammodels.generators.RandomCatastropheMetricGenerator;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;


/**
 * The Ride Cleansing exercise from the Flink training.
 *
 * <p>The task of this exercise is to filter a data stream of taxi ride records to keep only rides
 * that both start and end within New York City. The resulting stream should be printed.
 */
public class SdSigmaModel {

    private final DataGeneratorSource<Integer> sourceInt;
    private final DataGeneratorSource<Integer> sourceBernoulli;
    private final SinkFunction<Integer> sink;
    private final SinkFunction<String> sinkStr;


    /**
     * Creates a job using the source and sink provided.
     */
    public SdSigmaModel(DataGeneratorSource<Integer> sourceInt,
                        DataGeneratorSource<Integer> sourceBernoulli,
                        SinkFunction<Integer> sink) {
        this.sourceInt = sourceInt;
        this.sourceBernoulli = sourceBernoulli;
        this.sink = sink;
        this.sinkStr = new PrintSinkFunction<>();
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        SdSigmaModel job =
                new SdSigmaModel(
                        new DataGeneratorSource<>(
                                RandomCatastropheMetricGenerator.intCatastropheMetricGenerator(1, 10, 1000L)),
                        new DataGeneratorSource<>(ProcessGenerator.bernoulliProcessGenerator(0.5)),
                        new PrintSinkFunction<>());

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
        DataStream<Integer> metricWithCatastrophe = env.addSource(sourceInt, TypeInformation.of(Integer.class));

        WatermarkStrategy ws = WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(1))
                .withTimestampAssigner((event, timestamp) -> Instant.now().getEpochSecond());

        metricWithCatastrophe.assignTimestampsAndWatermarks(ws)
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new MeanCounter())
                .addSink(new PrintSinkFunction());



        return env.execute("Taxi Ride Cleansing");
    }

    /**
     * Keep only those rides and both start and end in NYC.
     */
    public class MeanCounter extends ProcessAllWindowFunction<Integer, Double, TimeWindow> {
        private ValueStateDescriptor<Double> meanStoreDescriptor =
                new ValueStateDescriptor<Double>("mean-store", Double.class);
        @Override
        public void process(ProcessAllWindowFunction<Integer, Double, TimeWindow>.Context context,
                            Iterable<Integer> elements, Collector<Double> out) throws Exception {
            ValueState<Double> meanStore = context.globalState().getState(meanStoreDescriptor);
            Double mean = meanStore.value();
            if (mean == null) mean = 0D;
            for (Integer element : elements) {
                Double toMean = element.doubleValue();
                mean = mean + toMean;
            };
            meanStore.update(mean);
            out.collect(mean);
        }
    }
}
