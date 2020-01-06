package com.github.hcsp;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
        }, 2);
    }

    // 并发执行tasks参数所指定的任务，并将任务的结果交给consumer线程串行处理
    // 任务执行期间若抛出任何异常，则在主线程中重新抛出它
    // 请回答：
    // 1. 这个方法运行的流程是怎么样的？运行的时候JVM中有几个线程？它们如何互相交互？
    //   开启消费线程对任务结果进行串行处理，用线程池中的线程并行处理任务，并将结果放入消费队列，最后处理异常；
    //   1个主线程 + n个任务线程 + 1个消费线程 + ?个VM自带线程；
    //   主线程创建任务线程和消费线程，任务线程中的结果放入消费线程中进行处理，
    //   消费线程中的异常捕获后在主线程中处理，主线程通过喂毒终止消费线程。
    // 2. 为什么有的时候会卡死？应该如何修复？
    //   消费线程遇到异常就终止了，消费队列积压，PoisonPill喂不进去；
    // 3. PoisonPill是什么东西？如果不懂的话可以搜索一下。
    //   用于终止消费线程的毒药，单例模式。
    public static <T> void runInParallelButConsumeInSerial(List<Callable<T>> tasks,
                                                           Consumer<T> consumer,
                                                           int numberOfThreads) throws Exception {
        BlockingQueue<Future<T>> queue = new LinkedBlockingQueue<>(numberOfThreads);
        Queue<Exception> exceptionInConsumerThreads = new LinkedList<>();

        Thread consumerThread = new Thread(() -> {
            while (true) {
                try {
                    Future<T> future = queue.poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                    if (future == PoisonPill.INSTANCE) {
                        break;
                    }

                    try {
                        assert future != null;
                        consumer.accept(future.get());
                    } catch (Exception e) {
                        exceptionInConsumerThreads.add(e);
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

        for (Exception e : exceptionInConsumerThreads) {
            throw e;
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
