package com.github.hcsp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class ExecutorTest {
    @Test
    @Timeout(5)
    public void runInParallelButConsumeInSerialTest() throws Exception {
        List<Integer> result = new ArrayList<>();
        Executor.runInParallelButConsumeInSerial(Arrays.asList(
                () -> {
                    easySleep(1500);
                    return 3;
                },
                () -> {
                    easySleep(1000);
                    return 2;
                },
                () -> {
                    easySleep(500);
                    return 1;
                }
        ), (Consumer<Integer>) result::add, 2);

        Assertions.assertEquals(Arrays.asList(3, 2, 1), result);
    }

    @Test
    @Timeout(5)
    public void exceptionTest() {
        Assertions.assertThrows(IllegalStateException.class, () ->
                Executor.runInParallelButConsumeInSerial(Arrays.asList(
                        () -> {
                            easySleep(1500);
                            return 3;
                        },
                        () -> {
                            easySleep(1000);
                            return 2;
                        },
                        () -> {
                            easySleep(500);
                            return 1;
                        }
                ), (Integer number) -> {
                    throw new IllegalStateException();
                }, 2)
        );
    }

    private void easySleep(int timeMillis) {
        try {
            Thread.sleep(timeMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
