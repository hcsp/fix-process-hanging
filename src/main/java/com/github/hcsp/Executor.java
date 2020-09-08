package com.github.hcsp;

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
//    public static void main(String[] args) throws Exception {
//        runInParallelButConsumeInSerial(Arrays.asList(
//                () -> 1,
//                () -> 2,
//                () -> 3
//        ), System.out::println, 2);
//
//        runInParallelButConsumeInSerial(Arrays.asList(
//                () -> 1,
//                () -> 2,
//                () -> 3
//        ), result -> {
//            throw new IllegalStateException();
//        }, 2);
//    }

    // 并发执行tasks参数所指定的任务，并将任务的结果交给consumer线程串行处理
    // 任务执行期间若抛出任何异常，则在主线程中重新抛出它
    // 请回答：
    // 1. 这个方法运行的流程是怎么样的？运行的时候JVM中有几个线程？它们如何互相交互？
    // 1.1 主线程创建消费者线程不断地从队列消费，然后创建生产者线程池，不断地执行任务并生产结果，最后向队列中放入毒丸PoisonPill，消费者取到PoisonPill时结束自己的线程
    // 1.2 3+个线程，主线程、消费者线程、生产者线程池，它们通过队列 queue 变量进行交互，主线程放入，生产者执行任务并产生结果，消费者从队列中取出结果
    // 2. 为什么有的时候会卡死？应该如何修复？
    // 2.1 因为在消费者在消费遇到异常时，终止了当前线程，导致后续生产者生产的线程没有消费，队列不断扩大导致卡死
    // 2.2 消费遇到异常时，不要终止线程，而是记录异常，在后续主逻辑中抛出即可
    // 3. PoisonPill 是什么东西？如果不懂的话可以搜索一下。
    // 3.1 PoisonPill 主要用于实现毒丸策略，当所有需要放入队列的值都放完是，最后放入PoisonPill ，当消费者读到 PoisonPill 时，便终止调消费者线程，从而实现优雅的关闭消费者
    public static <T> void runInParallelButConsumeInSerial(List<Callable<T>> tasks,
                                                            Consumer<T> consumer,
                                                            int numberOfThreads) throws Exception {
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
                        consumer.accept(future.get());
                    } catch (Exception e) {
                        exceptionInConsumerThread.set(e);
//                        break;
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
