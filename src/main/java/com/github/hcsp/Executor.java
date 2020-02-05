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
    // 首先启动消费者线程，从队列里面获取tasks的执行结果，然后调用consumer消费。
    // 然后，使用线程池去执行tasks，然后将future放进队列中，方便将结果传递给消费者线程。
    // 最后主线程使用队列发送PoisonPill结束信号给消费者线程，等待消费者线程结束，并把线程池关闭。
    //
    // 2个线程 + 1个线程池：main, customer, threadPool

    // 2. 为什么有的时候会卡死？应该如何修复？
    // 消费者线程由于异常break掉，而导致队列没人消费很快就满了，生产者再往队列put就会卡住。
    // 1). 使用无限队列。不过可能会导致内存泄漏。
    // 2). 如果队列满了就不往里面put了。但是如果只是消费者处理速度慢，就会导致tasks丢失。
    // 3). 使用queue.offer增加超时时间，一旦超时，就结束。不过超时时间的定夺就需要额外考量了。
    // 4). 发生异常后，消费者线程不退出。[Use this]
    // 5). 看ktlint做法，是使用当前线程作为consumer，一旦发生异常，把线程池关闭.

    // 3. PoisonPill是什么东西？如果不懂的话可以搜索一下。
    // 毒丸策略，在这里是通过队列来通知消费者线程结束的。

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

        // When consumerThread break and terminal, the queue will be filled up
        // And the queue.put() will make thread waiting until it be interrupted.
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
