/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.memory.jetty;

import io.airlift.units.DataSize;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.io.RetainableByteBuffer;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Integer.numberOfLeadingZeros;
import static java.lang.Math.floorMod;
import static java.lang.Math.max;
import static java.lang.Thread.currentThread;
import static java.util.Arrays.stream;

public class ConcurrentRetainableBufferPool
        implements ByteBufferPool
{
    private static final long DEFAULT_MAX_MEMORY = DataSize.of(1, MEGABYTE).toBytes();

    // Its not efficient in terms of memory overhead and re-use of buffers to have pools of very small allocation size like 1 or 2 bytes
    // The minimal buffer size should be large enough in that sense but small enough not to have too much overhead in case allocations are smaller
    // We chose 128 bytes
    private static final int MIN_POOL_SIZE_SHIFT = 7;
    private static final int[] poolSizeShiftToSize;

    private final ArenaBucket[] heapBuckets;
    private final ArenaBucket[] offHeapBuckets;
    private final AtomicBoolean evictor = new AtomicBoolean(false); // left from the original jetty code
    private final int numBuckets;
    private final int checkMaxMemoryPoint;

    private long maxHeapMemory;
    private long maxOffHeapMemory;
    private int checkCount;

    static {
        poolSizeShiftToSize = new int[Integer.SIZE - MIN_POOL_SIZE_SHIFT];
        for (int i = 0; i < poolSizeShiftToSize.length; i++) {
            poolSizeShiftToSize[i] = 1 << i + MIN_POOL_SIZE_SHIFT;
        }
    }

    public ConcurrentRetainableBufferPool(long maxHeapMemory, long maxOffHeapMemory)
    {
        this.numBuckets = Runtime.getRuntime().availableProcessors() * 4; // factor of number of tasks
        // its not efficent in terms of CPU to check max memory on every free, we heuristicly take a factor on the number of buckets
        // to decide on how many frees we count until we check. The 100 factor was taken from the original jetty code.
        this.checkMaxMemoryPoint = numBuckets * 100;
        // note that both max numbers are adaptive to avoid getting too many evictions. in case max buffer is larger, max memory is updated as well
        this.maxHeapMemory = maxHeapMemory > 0 ? maxHeapMemory : DEFAULT_MAX_MEMORY;
        this.maxOffHeapMemory = maxOffHeapMemory > 0 ? maxOffHeapMemory : DEFAULT_MAX_MEMORY;

        heapBuckets = new ArenaBucket[numBuckets];
        offHeapBuckets = new ArenaBucket[numBuckets];
        for (int bucketId = 0; bucketId < numBuckets; bucketId++) {
            heapBuckets[bucketId] = new ArenaBucket(false, bucketId);
            offHeapBuckets[bucketId] = new ArenaBucket(true, bucketId);
        }
    }

    @Override
    public RetainableByteBuffer acquire(int size, boolean offHeap)
    {
        int bucketId = floorMod(currentThread().threadId(), numBuckets);
        if (offHeap) {
            return offHeapBuckets[bucketId].alloc(size);
        }
        return heapBuckets[bucketId].alloc(size);
    }

    // similar to the original jetty code
    private void checkMaxMemory(boolean offHeap)
    {
        long max = offHeap ? maxOffHeapMemory : maxHeapMemory;
        if (max <= 0 || !evictor.compareAndSet(false, true)) {
            return;
        }

        try {
            checkCount++;
            if (checkCount % checkMaxMemoryPoint == 0 && getMemory(offHeap) > max) {
                evict(offHeap);
            }
        }
        finally {
            evictor.set(false);
        }
    }

    private void evict(boolean offHeap)
    {
        if (offHeap) {
            stream(offHeapBuckets).forEach(ArenaBucket::evict);
        }
        else {
            stream(heapBuckets).forEach(ArenaBucket::evict);
        }
    }

    private long getOffHeapMemory()
    {
        return getMemory(true);
    }

    private long getHeapMemory()
    {
        return getMemory(false);
    }

    private long getMemory(boolean offHeap)
    {
        if (offHeap) {
            return stream(offHeapBuckets)
                    .mapToLong(ArenaBucket::getMemory)
                    .sum();
        }
        return stream(heapBuckets)
                .mapToLong(ArenaBucket::getMemory)
                .sum();
    }

    @Override
    public void clear()
    {
        evict(true);
        evict(false);
    }

    @Override
    public String toString()
    {
        return String.format("%s{onHeap=%d/%d,offHeap=%d/%d}", super.toString(), getHeapMemory(), maxHeapMemory, getOffHeapMemory(), maxOffHeapMemory);
    }

    private class ArenaBucket
    {
        private Arena sharedArena;
        private final Arena autoArena;
        private final int bucketId;
        private final boolean offHeap;
        private final List<FixedSizeBufferPool> pools;

        ArenaBucket(boolean offHeap, int bucketId)
        {
            this.sharedArena = Arena.ofShared();
            this.autoArena = Arena.ofAuto();
            this.bucketId = bucketId;
            this.offHeap = offHeap;
            this.pools = new ArrayList<>();
        }

        synchronized RetainableByteBuffer alloc(int size)
        {
            int poolSizeShift = getPoolSizeShift(size);
            if (poolSizeShift >= pools.size()) {
                addNewPools(poolSizeShift);
            }
            // for avoiding pool size 32K (shift 8) please see https://bugs.openjdk.org/browse/JDK-8333849 which will be fixed in JDK 24
            return pools.get(poolSizeShift).allocate((offHeap && (poolSizeShift == 8)) ? autoArena : sharedArena);
        }

        private int getPoolSizeShift(int size)
        {
            return max(MIN_POOL_SIZE_SHIFT, Integer.SIZE - numberOfLeadingZeros(size - 1)) - MIN_POOL_SIZE_SHIFT;
        }

        private void addNewPools(int poolSizeShift)
        {
            int newPoolSizeShift;
            for (newPoolSizeShift = pools.size(); newPoolSizeShift <= poolSizeShift; newPoolSizeShift++) {
                pools.add(new FixedSizeBufferPool(poolSizeShiftToSize[newPoolSizeShift], offHeap));
            }
            updateMaxMemoryIfNeeded(poolSizeShiftToSize[newPoolSizeShift] * 16); // heuristically set the maximum to factor of the maximal buffer size
        }

        private void updateMaxMemoryIfNeeded(int newMaxSize)
        {
            if (offHeap) {
                if (newMaxSize > maxOffHeapMemory) {
                    maxOffHeapMemory = newMaxSize;
                }
            }
            else if (newMaxSize > maxHeapMemory) {
                maxHeapMemory = newMaxSize;
            }
        }

        synchronized void evict()
        {
            boolean canClose = offHeap;
            for (FixedSizeBufferPool pool : pools) {
                pool.evict();
                canClose &= pool.getBufferCount() == 0;
            }
            if (canClose) {
                sharedArena.close(); // free all memory associated with this arena
                sharedArena = Arena.ofShared(); // recreate the arena for new allocations
            }
        }

        synchronized long getMemory()
        {
            return pools.stream()
                    .mapToLong(FixedSizeBufferPool::getMemory)
                    .sum();
        }

        @Override
        public String toString()
        {
            return String.format("%s{bucketId=%d,offHeap=%b,#pools=%d}", super.toString(), bucketId, offHeap, pools.size());
        }
    }

    // similar to the original jetty code that held a bucket per power of 2
    private class FixedSizeBufferPool
    {
        private final List<MemorySegment> buffers;
        private final int bufferSize;
        private final boolean offHeap;
        private int allocatedBuffers;

        FixedSizeBufferPool(int bufferSize, boolean offHeap)
        {
            this.buffers = new ArrayList<>();
            this.bufferSize = bufferSize;
            this.offHeap = offHeap;
        }

        synchronized Buffer allocate(Arena arena)
        {
            // to be on the safe side, if we allocate from the list we allocate from the head free to the tail
            MemorySegment buffer = buffers.isEmpty() ? allocateNewBuffer(arena) : buffers.removeFirst();
            allocatedBuffers++;
            return new Buffer(buffer, this);
        }

        synchronized void free(MemorySegment buffer)
        {
            if (allocatedBuffers == 0) {
                throw new RuntimeException("pool has freed all allocated segments");
            }
            allocatedBuffers--;
            buffers.add(buffer);
        }

        synchronized void evict()
        {
            buffers.clear();
        }

        private MemorySegment allocateNewBuffer(Arena arena)
        {
            return offHeap ? arena.allocate(bufferSize, Integer.BYTES) : MemorySegment.ofArray(new byte[bufferSize]);
        }

        long getMemory()
        {
            return (allocatedBuffers + buffers.size()) * (long) bufferSize;
        }

        int getBufferCount()
        {
            return allocatedBuffers + buffers.size();
        }

        boolean isOffHeap()
        {
            return offHeap;
        }
    }

    // similar to the original jetty code with reference count per buffer for retain
    private class Buffer
            implements RetainableByteBuffer
    {
        private final AtomicInteger refCount;
        private final MemorySegment buffer;
        private final FixedSizeBufferPool pool;
        private ByteBuffer byteBuffer;

        Buffer(MemorySegment buffer, FixedSizeBufferPool pool)
        {
            this.refCount = new AtomicInteger(1);
            this.buffer = buffer;
            this.pool = pool;

            this.byteBuffer = buffer.asByteBuffer();
            byteBuffer.position(0); // this is a requirement to return the byte buffer with these attributes
            byteBuffer.limit(0); // this is a requirement to return the byte buffer with these attributes
        }

        @Override
        public void retain()
        {
            if (byteBuffer == null) {
                throw new IllegalStateException("buffer cannot be retained since already released");
            }
            refCount.getAndUpdate(c -> c + 1);
        }

        @Override
        public boolean release()
        {
            if (byteBuffer == null) {
                return true;
            }
            boolean shouldRelease = refCount.updateAndGet(c -> c - 1) == 0;
            if (shouldRelease) {
                pool.free(buffer);
                byteBuffer = null;

                checkMaxMemory(pool.isOffHeap());
            }
            return shouldRelease;
        }

        @Override
        public boolean canRetain()
        {
            return true;
        }

        @Override
        public boolean isRetained()
        {
            return refCount.get() > 1;
        }

        @Override
        public ByteBuffer getByteBuffer()
        {
            return byteBuffer;
        }
    }
}
