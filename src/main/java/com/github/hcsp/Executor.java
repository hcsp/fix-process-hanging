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
        }, 2);
    }

    // 并发执行tasks参数所指定的任务，并将任务的结果交给consumer线程串行处理
    // 任务执行期间若抛出任何异常，则在主线程中重新抛出它
    // 请回答：
    // 1. 这个方法运行的流程是怎么样的？运行的时候JVM中有几个线程？它们如何互相交互？
    //    流程：创建消费者阻塞队列，创建AtomicReference 赋值是原子操作CAS ，
    //         创建了消费者线程->访问阻塞队列获取进行消费->如果到了最后一个接终止
    //    线程: 主线程、消费者线程、线程池
    //    交互: 消费者线程通过 阻塞队列获取Future，如果到了最后一个就终止，但是消费出现异常线程就终止了.
    //          通过Executors执行器创建线程池并且向队列添加需要提交的任务，最后再提交一个PoisonPill，
    //          消费者线程结束再终止线程池，exceptionInConsumerThread消费时出现异常就进行输出
    //
    // 2. 为什么有的时候会卡死？应该如何修复？
    //    卡死原因: 因为消费者线程进行消费时出现异常 catch进行了保存异常的信息并且终止了线程！
    //    解决: 只进行异常保存 不终止线程。
    //
    // 3. PoisonPill是什么东西？如果不懂的话可以搜索一下。
    //   “毒丸”对象将确保消费者在关闭之前首先完成队列中的所有任务，
    //   “毒丸”对象之前提交的所有任务都会被处理，
    //    而生产者在提交了“毒丸”对象后，将不会再提交任何任务。
    //
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
