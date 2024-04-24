/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.benchmark.action.cluster.state;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Threads(1)
@State(Scope.Benchmark)
public class CollectionReferenceBenchmark {

    @Param({"1", "22", "80", "17500"})
    public int hashMapEntries;

    @Benchmark
    public void testCreatingAHashMapOfInts(Blackhole blackhole) {
        HashMap<Integer, Integer> map = new HashMap<>();
        for (int i = 0; i < hashMapEntries; i++) {
            map.put(i, i);
        }
        blackhole.consume(map);
    }

}
