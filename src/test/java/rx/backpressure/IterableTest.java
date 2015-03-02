package rx.backpressure;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.lang.Exception;import java.lang.String;import java.lang.System;import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * this test just simulate the backpressure pattern.
 * it doesn't really read anything from  kafka.
 */
@Ignore
public class IterableTest {

    @Test
    public void testSlowSyncSubscriber() throws Exception {
        final int expectedCount = 500;
        CountDownLatch latch = new CountDownLatch(expectedCount);
        Subscription s = Observable
                .from(new TestIterable(0, 1))
                .subscribeOn(Schedulers.newThread())
                .observeOn(Schedulers.newThread())
                .subscribe(new SyncSubscriber(250, latch, 5, false));

        long start = System.currentTimeMillis();
        boolean done = latch.await(10, TimeUnit.SECONDS);
        s.unsubscribe();
        System.out.println(String.format("took %d ms to process %d msgs",
                System.currentTimeMillis() - start, expectedCount - latch.getCount()));
        Assert.assertTrue(done);
    }

    @Test
    public void testSlowAsyncSubscriber() throws Exception {
        final int expectedCount = 1000;
        CountDownLatch latch = new CountDownLatch(expectedCount);
        Subscription s = Observable
                .from(new TestIterable(0, 1))
                .subscribeOn(Schedulers.newThread())
                .observeOn(Schedulers.newThread())
                .subscribe(new AsyncSubscriber(500, latch, 5, false));

        long start = System.currentTimeMillis();
        boolean done = latch.await(10, TimeUnit.SECONDS);
        s.unsubscribe();
        System.out.println(String.format("took %d ms to process %d msgs",
                System.currentTimeMillis() - start, expectedCount - latch.getCount()));
        Assert.assertTrue(done);
    }

}
