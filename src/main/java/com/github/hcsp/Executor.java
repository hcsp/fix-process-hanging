package com.github.hcsp;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    // 定义了定长的阻塞队列LinkedBlockingQueue，
    // 一共3种线程，main线程（也是生产者），消费线程和执行任务的线程
    // 执行任务的线程由缓存线程池创建，执行任务调用后，由main线程负责把Future放入阻塞队列
    // 消费线程会串行地消费阻塞队列里的信息。
    // 当消费线程遇到毒丸对象的时候，消费停止。

    // 2. 为什么有的时候会卡死？应该如何修复？
    // 在抛出异常的消费方法中，main线程在向阻塞队列中放毒丸对象的过程中卡死（WAITING状态）
    // 原因是消费线程在消费第一个future时，抛出异常退出了消费线程，导致无法继续消费，阻塞队列扔满了任务
    // 所以main线程在向阻塞队列中放毒丸对象时发现阻塞队列满了，于是等待消费
    // 修复方法就是，在消费线程中遇到异常不用break，处理后继续消费下一个

    // 3. PoisonPill是什么东西？如果不懂的话可以搜索一下。
    // “毒丸”对象将确保消费者在关闭之前首先完成队列中的所有任务，
    // “毒丸”对象之前提交的所有任务都会被处理，
    // 而生产者在提交了“毒丸”对象后，将不会再提交任何任务。
    public static <T> void runInParallelButConsumeInSerial(List<Callable<T>> tasks,
                                                           Consumer<T> consumer,
                                                           int numberOfThreads) throws Exception {
        BlockingQueue<Future<T>> queue = new LinkedBlockingQueue<>(numberOfThreads);
        List<Exception> exceptionsInConsumerThread = new CopyOnWriteArrayList<>();

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
                        exceptionsInConsumerThread.add(e);
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

        if (!exceptionsInConsumerThread.isEmpty()) {
            StringBuilder exceptionMsg = new StringBuilder();
            for (Exception e : exceptionsInConsumerThread) {
                String msg = Arrays.stream(e.getStackTrace())
                        .map(StackTraceElement::toString)
                        .collect(Collectors.toList())
                        .stream()
                        .collect(Collectors.joining(";", "", ""));
                exceptionMsg.append(msg);
            }
            throw new IllegalStateException(exceptionMsg.toString());
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
