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
    /*
      1. 这个方法运行的流程是怎么样的？运行的时候JVM中有几个线程？它们如何互相交互？
           将定义好的任务提交给线程池执行，然后把执行结果放到阻塞队列中；由新的线程来统一处理这个结果；
           5个：线程池中有三个线程执行任务；一个主线程；一个处理结果的线程
           线程池通过 queue 与处理结果的线程交互；如果处理中发现错误，会告诉主线程
     */
    /*
      2. 为什么有的时候会卡死？应该如何修复？
           程序卡死是因为queue里面有死锁。当程序抛出异常后，break会使用整个消费者线程死亡；
           此时，由于queue里面已经满了，主线程再往queue添加，就需要等消费者线程去获取queue，而消费者线程又已经死亡了；
           所以主线程就一直等待queue.poll释放锁
     */
    /*
      3. PoisonPill是什么东西？如果不懂的话可以搜索一下。
            PoisonPill 【毒丸防御】，是用于防止公司被恶意收购的一种手段；
            在程序中，一般是用来通知其他线程，这里有不良的事情发生；
     */
    public static <T> void runInParallelButConsumeInSerial(List<Callable<T>> tasks,
                                                           Consumer<T> consumer,
                                                           int numberOfThreads) throws Exception {
        // 任务队列
        BlockingQueue<Future<T>> queue = new LinkedBlockingQueue<>(numberOfThreads);
        AtomicReference<Exception> exceptionInConsumerThread = new AtomicReference<>();

        // 得到线程池的处理结果，并将其在这个线程处理
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
