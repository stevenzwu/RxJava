package rx.backpressure;

import org.junit.Assert;
import org.junit.Test;
import rx.Observable;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.observables.GroupedObservable;
import rx.schedulers.Schedulers;

import java.lang.Exception;import java.lang.Integer;import java.lang.Override;import java.lang.String;import java.lang.System;import java.lang.Throwable;import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class IterableMultiStreamTest {
    @Test
    public void testSlowSyncSubscriber() throws Exception {
        final int expectedCount = 500;
        CountDownLatch latch = new CountDownLatch(expectedCount);
        Subscription s = createStream(2, 4, 1).subscribe(new SyncSubscriber(250, latch, 50, false));
        long start = System.currentTimeMillis();
        boolean done = latch.await(10, TimeUnit.SECONDS);
        s.unsubscribe();
        long duration = System.currentTimeMillis() - start;
        System.out.println(String.format("took %d ms to process %d msgs",
                duration, expectedCount - latch.getCount()));
        Assert.assertTrue(done);
        // 2x4 processors, each can process 20 items/second
        // so 500 items should take [3-4]s
        Assert.assertTrue(duration > TimeUnit.SECONDS.toMillis(2));
        Assert.assertTrue(duration < TimeUnit.SECONDS.toMillis(5));
    }

    @Test
    public void testSlowAsyncSubscriber() throws Exception {
        final int expectedCount = 1000;
        CountDownLatch latch = new CountDownLatch(expectedCount);
        Subscription s = createStream(2, 4, 1).subscribe(new AsyncSubscriber(500, latch, 50, false));
        // 2x4 processors, each can process 20 items/second
        // so 500 items should take less than 4s
        // async should be even faster with large intial requested
        long start = System.currentTimeMillis();
        boolean done = latch.await(10, TimeUnit.SECONDS);
        s.unsubscribe();
        long duration = System.currentTimeMillis() - start;
        System.out.println(String.format("took %d ms to process %d msgs",
                duration, expectedCount - latch.getCount()));
        Assert.assertTrue(done);
        // with async it should be less than 2s
        Assert.assertTrue(duration < TimeUnit.SECONDS.toMillis(2));
    }

    @Test
    public void testSlowSourcesWithSyncSubscriber() throws Exception {
        final int expectedCount = 500;
        CountDownLatch latch = new CountDownLatch(expectedCount);
        Subscription s = createStream(4, 1, 20).subscribe(new SyncSubscriber(250, latch, 1, false));
        long start = System.currentTimeMillis();
        boolean done = latch.await(10, TimeUnit.SECONDS);
        s.unsubscribe();
        long duration = System.currentTimeMillis() - start;
        System.out.println(String.format("took %d ms to process %d msgs",
                duration, expectedCount - latch.getCount()));
        Assert.assertTrue(done);
        // 4 readers, each can process 50 items/second
        // so 500 items should take [2-3]
        Assert.assertTrue(duration > TimeUnit.SECONDS.toMillis(2));
        Assert.assertTrue(duration < TimeUnit.SECONDS.toMillis(4));
    }

    @Test
    public void testSlowSourcesWithAsyncSubscriber() throws Exception {
        final int expectedCount = 500;
        CountDownLatch latch = new CountDownLatch(expectedCount);
        Subscription s = createStream(4, 1, 20).subscribe(new AsyncSubscriber(250, latch, 1, false));
        long start = System.currentTimeMillis();
        boolean done = latch.await(10, TimeUnit.SECONDS);
        s.unsubscribe();
        long duration = System.currentTimeMillis() - start;
        System.out.println(String.format("took %d ms to process %d msgs",
                duration, expectedCount - latch.getCount()));
        Assert.assertTrue(done);
        // 4 readers, each can process 50 items/second
        // so 500 items should take [2-3]
        Assert.assertTrue(duration > TimeUnit.SECONDS.toMillis(2));
        Assert.assertTrue(duration < TimeUnit.SECONDS.toMillis(4));
    }

    private Observable<TestAckable> createStream(final int readerCount, final int processorCount, final long delay) {
        final Random r = new Random();
        final Observable<Observable<TestAckable>> sourceStreams = Observable.create(new Observable.OnSubscribe<Observable<TestAckable>>() {
            @Override
            public void call(final Subscriber<? super Observable<TestAckable>> subscriber) {

                Observable.range(0, readerCount)
                        .map(new Func1<Integer, TestIterable>() {
                            @Override
                            public TestIterable call(Integer integer) {
                                return new TestIterable(integer, delay);
                            }
                        })
                        .subscribe(new Action1<TestIterable>() {
                            @Override
                            public void call(TestIterable kafkaIterable) {
                                Observable<TestAckable> sourceStream = Observable.from(kafkaIterable)
                                        .subscribeOn(Schedulers.newThread());
                                subscriber.onNext(sourceStream);
                            }
                        });
            }
        });

        final Observable<Observable<TestAckable>> processorStreams = sourceStreams
                .flatMap(new Func1<Observable<TestAckable>, Observable<Observable<TestAckable>>>() {
                    @Override
                    public Observable<Observable<TestAckable>> call(Observable<TestAckable> o) {
                        return o
                                .groupBy(new Func1<TestAckable, Integer>() {
                                    @Override
                                    public Integer call(TestAckable ackable) {
                                        return r.nextInt(processorCount) % processorCount;
                                    }
                                })
                                .map(new Func1<GroupedObservable<Integer, TestAckable>, Observable<TestAckable>>() {
                                    @Override
                                    public Observable<TestAckable> call(
                                            GroupedObservable<Integer, TestAckable> shardedObservable) {
                                        System.out.println("Starting processing thread: " + shardedObservable.getKey());
                                        return shardedObservable.observeOn(Schedulers.newThread());
                                    }
                                });
                    }
                });

        return Observable.create(new Observable.OnSubscribe<TestAckable>() {
            @Override
            public void call(final Subscriber<? super TestAckable> mainObserver) {
                processorStreams.subscribe(new Action1<Observable<TestAckable>>() {
                    @Override
                    public void call(Observable<TestAckable> paralellObservable) {
                        paralellObservable
                                .subscribe(new Observer<TestAckable>() {
                                    @Override
                                    public void onCompleted() {
                                        if (!mainObserver.isUnsubscribed()) {
                                            mainObserver.onCompleted();
                                        }
                                    }

                                    @Override
                                    public void onError(Throwable e) {
                                        if (!mainObserver.isUnsubscribed())
                                            mainObserver.onError(e);
                                    }

                                    @Override
                                    public void onNext(TestAckable t) {
                                        if (!mainObserver.isUnsubscribed())
                                            mainObserver.onNext(t);
                                    }
                                });
                    }
                });
            }
        });
    }
}
