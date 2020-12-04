package com.github.sukhinin.demo;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ConcurrentAccessDetector {

    private final Lock lock = new ReentrantLock();

    public void throwOnConcurrentAccess() {
        final boolean locked = lock.tryLock();
        if (!locked) {
            throw new IllegalStateException("Concurrent access detected!");
        }

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // Ignore
        } finally {
            lock.unlock();
        }
    }
}
