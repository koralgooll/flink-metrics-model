/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streammodels.generators;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

import java.sql.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/** Random generator. */
@Experimental
public abstract class RandomCatastropheMetricGenerator<T> implements DataGenerator<T> {

    protected transient RandomDataGenerator random;
    protected transient Timer catastropheTimer;
    protected Boolean isCatastrophe = false;
    protected Long delay = 1000L;

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        this.random = new RandomDataGenerator();
        this.catastropheTimer = new Timer();
        this.catastropheTimer.schedule(new TimerTask() {
            @Override
            public void run() {
                isCatastrophe = true;
            }
        }, delay);
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    private RandomCatastropheMetricGenerator(Long delay){
        this.delay = delay;
    }

    public static RandomCatastropheMetricGenerator<Integer> intCatastropheMetricGenerator(
            int min, int max, Long delay) {
        return new RandomCatastropheMetricGenerator<Integer>(delay) {

            @Override
            public Integer next() {
                try {
                    Thread.sleep(Double.valueOf(0.1 * 1000).longValue());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                Integer nextInteger = random.nextInt(min, max);
                if (this.isCatastrophe) {
                    nextInteger = nextInteger * 10;
                }
                return nextInteger;
            }
        };
    }
}
