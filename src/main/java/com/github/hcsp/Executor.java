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
    // 答：1. 首先是一个消费者线程循环的从BlockingQueue中拉取Future，并调用它的get方法让它阻塞直到有结果返回时让consumer去处理它。
    //    如果拉去的Future是我们自己实现的PoisonPill就代表结果队列拉取完毕跳出循环。然后就是用一个缓冲的线程池CachedThreadPool
    //    去提交任务，该线程池实现会为我们每一个任务开启一个线程去执行，并将返回的Future put进BlockingQueue，如果队列满的话，
    //    该方法会产生阻塞，另外我们put了一个PoisonPill，当consumer获取到了PoisonPill就代表已经走到了队尾结果已经全部处理完成。
    //    最后让主线程等待consumer线程执行完成。
    //     2. 一个main线程，一个consumer线程，工作线程的数量视情况而定，如果task数量少于我们定义的numberOfThreads值的话，有多少
    //        个任务就会有多少个线程被创建，总的情况应该是 Math.min(tasks.size(), numberOfThreads);
    //     3. 工作线程是由线程池产生的，它会返回一个Future代表线程执行任务产生的结果，调用它的get方法获取具体的结果值，该方法
    //      会产生阻塞知道线程执行完成有结果返回。consumer线程就是执行Future.get产生阻塞效果知道拿到等待的结果。
    //      简单说就是ExecutorService产生的结果交给consumer，而我们的main线程会一直等待consumer直到它执行完成。
    //
    // 2. 为什么有的时候会卡死？应该如何修复？
    //  答：问题在于当Future.get抛出异常时，我们的consumer捕获到了异常会执行break，跳出循环，没有人get了，而我们存放Future的
    //  BlockingQueue容量是有限制的，当他满了后put操作就会一只阻塞。
    //     修复：捕获到了异常后不要跳出循环，而应该让他继续循环,如果愿意的话可以用一个list把异常收集起来。

    // 3. PoisonPill是什么东西？如果不懂的话可以搜索一下。
    //   答： 字面意思是 毒丸，可以用与一个线程向另一个线程发送终止信号。把这个毒丸放在队列中代表是这个队列中最后一个事件，当其它
    //       线程吃到了这个"毒丸"就会跟人一样死翘翘，也就是终止。
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
            System.out.println("嘿嘿嘿.......有异常了吧");
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
