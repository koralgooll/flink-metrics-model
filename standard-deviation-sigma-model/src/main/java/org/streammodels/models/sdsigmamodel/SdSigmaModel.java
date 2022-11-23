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

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.util.Precision;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.sampling.BernoulliSampler;
import org.apache.flink.api.java.sampling.RandomSampler;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava30.com.google.common.collect.Collections2;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
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
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;


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
                                RandomCatastropheMetricGenerator.intCatastropheMetricGenerator(1, 10, 50000L)),
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
                .process(new ToString(), TypeInformation.of(String.class)).addSink(new PrintSinkFunction<String>());



        return env.execute("Taxi Ride Cleansing");
    }

    /**
     * Keep only those rides and both start and end in NYC.
     */
    public class MeanCounter extends ProcessAllWindowFunction<Integer, Tuple2<Double, Double>, TimeWindow> {
        private ValueStateDescriptor<Double> meanStoreDescriptor;
        private ValueStateDescriptor<Double> windowMeanStoreDescriptor;

        private ListStateDescriptor<Integer> fewWindowsStoreDescriptor;

        public MeanCounter() {
            this.meanStoreDescriptor = meanStoreDescriptor =
                    new ValueStateDescriptor<Double>("mean-store", Double.class);
            this.windowMeanStoreDescriptor =
                    new ValueStateDescriptor<Double>("win-mean-store", Double.class);
            this.fewWindowsStoreDescriptor =
                    new ListStateDescriptor<Integer>("few-win-elements-store", Integer.class);

            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(org.apache.flink.api.common.time.Time.seconds(30))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                    .build();
            this.fewWindowsStoreDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void process(ProcessAllWindowFunction<Integer, Tuple2<Double, Double>, TimeWindow>.Context context,
                            Iterable<Integer> elements, Collector<Tuple2<Double, Double>> out) throws Exception {
            ValueState<Double> meanStore = context.globalState().getState(meanStoreDescriptor);
            ValueState<Double> windowMeanStore = context.windowState().getState(windowMeanStoreDescriptor);


            ListState<Integer> fewWinElementsStore = context.globalState().getListState(fewWindowsStoreDescriptor);

            Double mean = windowMeanStore.value();
//            StreamSupport.stream(elements.spliterator(), false).count();
            ArrayList<Integer> fewWinElements = Lists.newArrayList(fewWinElementsStore.get());

            // TODO : Add to model params.
            Double samplingRation = 0.1;
            Integer elementsNo = Iterables.size(elements);
            Integer samplingElementsNo = Double.valueOf(Math.floor(samplingRation * elementsNo)).intValue();


            Random randomGen = new Random();
            ArrayList chosenElementsIds = new ArrayList();
            for (int i = 0; i < samplingElementsNo; i++) {
                chosenElementsIds.add(randomGen.nextInt(elementsNo));
            }

            if (mean == null) mean = 0D;
            Integer elementsCounter = 0;
            ArrayList<Integer> chosenElements = new ArrayList<Integer>();
            for (Integer element : elements) {
                if (chosenElementsIds.contains(elementsCounter)) {
                    chosenElements.add(element);
                }
                ++elementsCounter;
                Double toMean = element.doubleValue();
                mean = mean + toMean;

            };

            fewWinElementsStore.addAll(chosenElements);
            windowMeanStore.update(mean/elementsNo);

            // TODO : BUG : Global should use global state, window chosen elements.
            StandardDeviation globalSd = new StandardDeviation();
            double[] globalValues = new double[chosenElements.size()];
            int i=0;
            for(Integer value: chosenElements) {
                globalValues[i++] = value;
            }
            Double globalSdValue = Double.valueOf(globalSd.evaluate(globalValues));
            globalSdValue = Precision.round(globalSdValue.doubleValue(), 4);

            StandardDeviation windowSd = new StandardDeviation();
            double[] windowValues = new double[fewWinElements.size()];
            int j=0;
            for(Integer value: fewWinElements) {
                windowValues[j++] = value;
            }
            Double windowSdValue = Double.valueOf(windowSd.evaluate(windowValues));
            windowSdValue = Precision.round(windowSdValue.doubleValue(), 4);

            out.collect(new Tuple2<Double, Double>(globalSdValue, windowSdValue));
        }
    }

    private class ToString extends org.apache.flink.streaming.api.functions.ProcessFunction {
        @Override
        public void processElement(Object value, Context ctx, Collector out) throws Exception {
            out.collect("(global_sd, local_sd) : " + value.toString());
        }
    }
}
