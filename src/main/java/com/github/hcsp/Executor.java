package com.github.hcsp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class Executor {
    public static void main(String[] args) throws Exception {
        runInParallelButConsumeInSerial(Arrays.asList(
                () -> 1,
                () -> 2,
                () -> 3
        ), System.out::println, 2);

        runInParallelButConsumeInSerial(Arrays.asList(
                () -> 1,
                () -> 2,
                () -> 3
        ), result -> {
            throw new IllegalStateException();
        }, 3);
    }
    // 并发执行tasks参数所指定的任务，并将任务的结果交给consumer线程串行处理
    // 任务执行期间若抛出任何异常，则在主线程中重新抛出它
    // 请回答：
    // 1. 这个方法运行的流程是怎么样的？运行的时候JVM中有几个线程？它们如何互相交互？
    // 流程：
    //      1. 运行 consumerThread，在死循环中在 BlockingQueue 中 poll 值，当前 BlockingQueue 中没有值，则等待，
    //         拿到值如果是 PoisonPill.instance 则退出循环
    //         值如果不是 PoisonPill.instance 则丢给 consumer 去消费，如果报错
    //      2. 在主线程中声明线程池，之后将 Callable 丢给线程池，将 Future 丢到 BlockingQueue 中，最后丢入 PoisonPill.instance
    //         之后等待 consumerThread die。之后关闭线程池
    // 运行时线程：1. 主线程 2. consumerThread 3. 线程池
    // 线程之间交互：
    //          1. consumerThread 从 BlockingQueue 中获取值，之后 consumer
    //          2. 主线程将 Callable 丢到线程池中，之后等待 consumerThread die
    //          3. 线程池将 Future 推到 BlockingQueue 中
    // 2. 为什么有的时候会卡死？应该如何修复？
    //          程序会在运行第二个抛错的 consumer 的时候会卡死。BlockingQueue 有两个空间，如果空间满了则需要
    //          消费才能继续 put。当第一个抛错的 consumer 执行了之后，consumerThread 退出死循环，die。此时
    //          没有了消费者，BlockQueue 继续 put，一个有3个 Callable，3个 Callable 之后，BlockQueue 已
    //          经满了，此时再次 put 则会 wait，如同卡死
    //          BlockingQueue 满了，此时在等待消费
    //    修复方法：如果 consumerThread 遇到错误不 break，继续 poll，但是不消费
    // 3. PoisonPill是什么东西？如果不懂的话可以搜索一下。 => Poison Pill is known predefined data item that allows to provide graceful shutdown for separate distributed consumption process.
    public static <T> void runInParallelButConsumeInSerial(List<Callable<T>> tasks, Consumer<T> consumer, int numberOfThreads) throws Exception {
        BlockingQueue<Future<T>> queue = new LinkedBlockingQueue<>(numberOfThreads);
        AtomicReference<Exception> exceptionInConsumerThread = new AtomicReference<>();

        Thread consumerThread = new Thread(() -> {
            while (true) {
                try {
                    Future<T> future = queue.poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    if (future == PoisonPill.INSTANCE) {
                        break;
                    }

                    try {
                        if (exceptionInConsumerThread.get() == null) {
                            consumer.accept(future.get());
                        }
                    } catch (Exception e) {
                        exceptionInConsumerThread.set(e);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        consumerThread.start();

        ExecutorService threadPool = Executors.newCachedThreadPool();
        for (Callable<T> task : tasks) {
            queue.put(threadPool.submit(task));
        }

        queue.put((Future) PoisonPill.INSTANCE);

        consumerThread.join();

        threadPool.shutdown();

        if (exceptionInConsumerThread.get() != null) {
            throw exceptionInConsumerThread.get();
        }
    }

    private enum PoisonPill implements Future<Object> {
        INSTANCE;

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isCancelled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isDone() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            throw new UnsupportedOperationException();
        }
    }
}

/**
 * 2022-07-18 19:54:28
 * Full thread dump Java HotSpot(TM) 64-Bit Server VM (11.0.13+10-LTS-370 mixed mode):
 * <p>
 * Threads class SMR info:
 * _java_thread_list=0x0000600000bf3b60, length=13, elements={
 * 0x00007faf1680b000, 0x00007faf1883a000, 0x00007faf17853000, 0x00007faf1681d800,
 * 0x00007faf1681b000, 0x00007faf1681c000, 0x00007faf0692f800, 0x00007faf06930800,
 * 0x00007faf18865000, 0x00007faf178e1000, 0x00007faf18097000, 0x00007faf1681d000,
 * 0x00007faf1802a000
 * }
 * <p>
 * "main" #1 prio=5 os_prio=31 cpu=175.42ms elapsed=34.96s tid=0x00007faf1680b000 nid=0x1f03 waiting on condition  [0x000000030a210000]
 * java.lang.Thread.State: WAITING (parking)
 * at jdk.internal.misc.Unsafe.park(java.base@11.0.13/Native Method)
 * - parking to wait for  <0x000000061fc654a8> (a java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject)
 * at java.util.concurrent.locks.LockSupport.park(java.base@11.0.13/LockSupport.java:194)
 * at java.util.concurrent.locks.AbstractQueuedSynchronizer$ConditionObject.await(java.base@11.0.13/AbstractQueuedSynchronizer.java:2081)
 * at java.util.concurrent.LinkedBlockingQueue.put(java.base@11.0.13/LinkedBlockingQueue.java:341)
 * at com.github.hcsp.Executor.runInParallelButConsumeInSerial(Executor.java:93)
 * at com.github.hcsp.Executor.main(Executor.java:25)
 * <p>
 * "Reference Handler" #2 daemon prio=10 os_prio=31 cpu=0.13ms elapsed=34.94s tid=0x00007faf1883a000 nid=0x3b03 waiting on condition  [0x000000030a925000]
 * java.lang.Thread.State: RUNNABLE
 * at java.lang.ref.Reference.waitForReferencePendingList(java.base@11.0.13/Native Method)
 * at java.lang.ref.Reference.processPendingReferences(java.base@11.0.13/Reference.java:241)
 * at java.lang.ref.Reference$ReferenceHandler.run(java.base@11.0.13/Reference.java:213)
 * <p>
 * "Finalizer" #3 daemon prio=8 os_prio=31 cpu=0.65ms elapsed=34.94s tid=0x00007faf17853000 nid=0x3e03 in Object.wait()  [0x000000030aa28000]
 * java.lang.Thread.State: WAITING (on object monitor)
 * at java.lang.Object.wait(java.base@11.0.13/Native Method)
 * - waiting on <0x000000061fe09018> (a java.lang.ref.ReferenceQueue$Lock)
 * at java.lang.ref.ReferenceQueue.remove(java.base@11.0.13/ReferenceQueue.java:155)
 * - waiting to re-lock in wait() <0x000000061fe09018> (a java.lang.ref.ReferenceQueue$Lock)
 * at java.lang.ref.ReferenceQueue.remove(java.base@11.0.13/ReferenceQueue.java:176)
 * at java.lang.ref.Finalizer$FinalizerThread.run(java.base@11.0.13/Finalizer.java:170)
 * <p>
 * "Signal Dispatcher" #4 daemon prio=9 os_prio=31 cpu=0.58ms elapsed=34.93s tid=0x00007faf1681d800 nid=0xa003 runnable  [0x0000000000000000]
 * java.lang.Thread.State: RUNNABLE
 * <p>
 * "Service Thread" #5 daemon prio=9 os_prio=31 cpu=0.04ms elapsed=34.93s tid=0x00007faf1681b000 nid=0x9e03 runnable  [0x0000000000000000]
 * java.lang.Thread.State: RUNNABLE
 * <p>
 * "C2 CompilerThread0" #6 daemon prio=9 os_prio=31 cpu=44.37ms elapsed=34.93s tid=0x00007faf1681c000 nid=0x9c03 waiting on condition  [0x0000000000000000]
 * java.lang.Thread.State: RUNNABLE
 * No compile task
 * <p>
 * "C1 CompilerThread0" #9 daemon prio=9 os_prio=31 cpu=53.67ms elapsed=34.93s tid=0x00007faf0692f800 nid=0x9a03 waiting on condition  [0x0000000000000000]
 * java.lang.Thread.State: RUNNABLE
 * No compile task
 * <p>
 * "Sweeper thread" #10 daemon prio=9 os_prio=31 cpu=0.06ms elapsed=34.93s tid=0x00007faf06930800 nid=0x5d03 runnable  [0x0000000000000000]
 * java.lang.Thread.State: RUNNABLE
 * <p>
 * "Common-Cleaner" #11 daemon prio=8 os_prio=31 cpu=0.14ms elapsed=34.88s tid=0x00007faf18865000 nid=0x6103 in Object.wait()  [0x000000030b149000]
 * java.lang.Thread.State: TIMED_WAITING (on object monitor)
 * at java.lang.Object.wait(java.base@11.0.13/Native Method)
 * - waiting on <0x000000061fe75f50> (a java.lang.ref.ReferenceQueue$Lock)
 * at java.lang.ref.ReferenceQueue.remove(java.base@11.0.13/ReferenceQueue.java:155)
 * - waiting to re-lock in wait() <0x000000061fe75f50> (a java.lang.ref.ReferenceQueue$Lock)
 * at jdk.internal.ref.CleanerImpl.run(java.base@11.0.13/CleanerImpl.java:148)
 * at java.lang.Thread.run(java.base@11.0.13/Thread.java:834)
 * at jdk.internal.misc.InnocuousThread.run(java.base@11.0.13/InnocuousThread.java:134)
 * <p>
 * "Monitor Ctrl-Break" #12 daemon prio=5 os_prio=31 cpu=20.84ms elapsed=34.79s tid=0x00007faf178e1000 nid=0x6303 runnable  [0x000000030b24c000]
 * java.lang.Thread.State: RUNNABLE
 * at java.net.SocketInputStream.socketRead0(java.base@11.0.13/Native Method)
 * at java.net.SocketInputStream.socketRead(java.base@11.0.13/SocketInputStream.java:115)
 * at java.net.SocketInputStream.read(java.base@11.0.13/SocketInputStream.java:168)
 * at java.net.SocketInputStream.read(java.base@11.0.13/SocketInputStream.java:140)
 * at sun.nio.cs.StreamDecoder.readBytes(java.base@11.0.13/StreamDecoder.java:284)
 * at sun.nio.cs.StreamDecoder.implRead(java.base@11.0.13/StreamDecoder.java:326)
 * at sun.nio.cs.StreamDecoder.read(java.base@11.0.13/StreamDecoder.java:178)
 * - locked <0x000000061fc94960> (a java.io.InputStreamReader)
 * at java.io.InputStreamReader.read(java.base@11.0.13/InputStreamReader.java:181)
 * at java.io.BufferedReader.fill(java.base@11.0.13/BufferedReader.java:161)
 * at java.io.BufferedReader.readLine(java.base@11.0.13/BufferedReader.java:326)
 * - locked <0x000000061fc94960> (a java.io.InputStreamReader)
 * at java.io.BufferedReader.readLine(java.base@11.0.13/BufferedReader.java:392)
 * at com.intellij.rt.execution.application.AppMainV2$1.run(AppMainV2.java:49)
 * <p>
 * "pool-2-thread-1" #17 prio=5 os_prio=31 cpu=0.16ms elapsed=34.75s tid=0x00007faf18097000 nid=0x6b0f waiting on condition  [0x000000030b555000]
 * java.lang.Thread.State: TIMED_WAITING (parking)
 * at jdk.internal.misc.Unsafe.park(java.base@11.0.13/Native Method)
 * - parking to wait for  <0x000000061fc65798> (a java.util.concurrent.SynchronousQueue$TransferStack)
 * at java.util.concurrent.locks.LockSupport.parkNanos(java.base@11.0.13/LockSupport.java:234)
 * at java.util.concurrent.SynchronousQueue$TransferStack.awaitFulfill(java.base@11.0.13/SynchronousQueue.java:462)
 * at java.util.concurrent.SynchronousQueue$TransferStack.transfer(java.base@11.0.13/SynchronousQueue.java:361)
 * at java.util.concurrent.SynchronousQueue.poll(java.base@11.0.13/SynchronousQueue.java:937)
 * at java.util.concurrent.ThreadPoolExecutor.getTask(java.base@11.0.13/ThreadPoolExecutor.java:1053)
 * at java.util.concurrent.ThreadPoolExecutor.runWorker(java.base@11.0.13/ThreadPoolExecutor.java:1114)
 * at java.util.concurrent.ThreadPoolExecutor$Worker.run(java.base@11.0.13/ThreadPoolExecutor.java:628)
 * at java.lang.Thread.run(java.base@11.0.13/Thread.java:834)
 * <p>
 * "pool-2-thread-2" #18 prio=5 os_prio=31 cpu=0.13ms elapsed=34.75s tid=0x00007faf1681d000 nid=0x7403 waiting on condition  [0x000000030b658000]
 * java.lang.Thread.State: TIMED_WAITING (parking)
 * at jdk.internal.misc.Unsafe.park(java.base@11.0.13/Native Method)
 * - parking to wait for  <0x000000061fc65798> (a java.util.concurrent.SynchronousQueue$TransferStack)
 * at java.util.concurrent.locks.LockSupport.parkNanos(java.base@11.0.13/LockSupport.java:234)
 * at java.util.concurrent.SynchronousQueue$TransferStack.awaitFulfill(java.base@11.0.13/SynchronousQueue.java:462)
 * at java.util.concurrent.SynchronousQueue$TransferStack.transfer(java.base@11.0.13/SynchronousQueue.java:361)
 * at java.util.concurrent.SynchronousQueue.poll(java.base@11.0.13/SynchronousQueue.java:937)
 * at java.util.concurrent.ThreadPoolExecutor.getTask(java.base@11.0.13/ThreadPoolExecutor.java:1053)
 * at java.util.concurrent.ThreadPoolExecutor.runWorker(java.base@11.0.13/ThreadPoolExecutor.java:1114)
 * at java.util.concurrent.ThreadPoolExecutor$Worker.run(java.base@11.0.13/ThreadPoolExecutor.java:628)
 * at java.lang.Thread.run(java.base@11.0.13/Thread.java:834)
 * <p>
 * "Attach Listener" #19 daemon prio=9 os_prio=31 cpu=26.66ms elapsed=33.92s tid=0x00007faf1802a000 nid=0x6f17 waiting on condition  [0x0000000000000000]
 * java.lang.Thread.State: RUNNABLE
 * <p>
 * "VM Thread" os_prio=31 cpu=3.10ms elapsed=34.95s tid=0x00007faee6815000 nid=0x3903 runnable
 * <p>
 * "GC Thread#0" os_prio=31 cpu=1.35ms elapsed=34.96s tid=0x00007faf0680c000 nid=0x3203 runnable
 * <p>
 * "G1 Main Marker" os_prio=31 cpu=0.36ms elapsed=34.96s tid=0x00007faf18838800 nid=0x4b03 runnable
 * <p>
 * "G1 Conc#0" os_prio=31 cpu=0.04ms elapsed=34.96s tid=0x00007faf18839800 nid=0x3503 runnable
 * <p>
 * "G1 Refine#0" os_prio=31 cpu=0.21ms elapsed=34.96s tid=0x00007faf0692c800 nid=0x3603 runnable
 * <p>
 * "G1 Young RemSet Sampling" os_prio=31 cpu=4.87ms elapsed=34.96s tid=0x00007faf18011000 nid=0x3703 runnable
 * "VM Periodic Task Thread" os_prio=31 cpu=21.15ms elapsed=34.79s tid=0x00007faf06956000 nid=0x660f waiting on condition
 * <p>
 * JNI global refs: 16, weak refs: 0
 */
