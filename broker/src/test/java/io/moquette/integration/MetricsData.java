package io.moquette.integration;

import io.netty.buffer.PooledByteBufAllocatorMetric;

final class MetricsData {

    private final long usedDirectMemory;
    private final long usedHeapMemory;

    private MetricsData(long usedDirectMemory, long usedHeapMemory) {
        this.usedDirectMemory = usedDirectMemory;
        this.usedHeapMemory = usedHeapMemory;
    }

    static MetricsData snapshot(PooledByteBufAllocatorMetric metric) {
        return new MetricsData(metric.usedDirectMemory(), metric.usedHeapMemory());
    }

    /** Returns the total bytes that grew between {@code before} and this snapshot. */
    long leakedBytes(MetricsData before) {
        return (usedDirectMemory - before.usedDirectMemory) + (usedHeapMemory - before.usedHeapMemory);
    }

    long usedDirectMemory() {
        return usedDirectMemory;
    }

    long usedHeapMemory() {
        return usedHeapMemory;
    }
}
