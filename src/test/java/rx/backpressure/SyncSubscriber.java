package rx.backpressure;

import rx.Subscriber;
import rx.functions.Action1;

import java.lang.InterruptedException;import java.lang.Override;import java.lang.String;import java.lang.System;import java.lang.Thread;import java.lang.Throwable;import java.util.concurrent.CountDownLatch;

class SyncSubscriber extends Subscriber<TestAckable> {
    private final long initRequested;
    private final CountDownLatch latch;
    private final long delay;
    private final boolean trace;

    public SyncSubscriber(final long initRequested, final CountDownLatch latch,
                          final long delay, final boolean trace) {
        this.initRequested = initRequested;
        this.latch = latch;
        this.delay = delay;
        this.trace = trace;
    }

    @Override
    public void onStart() {
        System.out.println(String.format("onStart requested %d items", initRequested));
        request(initRequested);
    }

    @Override
    public void onCompleted() {
        System.out.println("onCompleted");
    }

    @Override
    public void onError(Throwable e) {
        System.out.println("onError");
        e.printStackTrace();
    }

    @Override
    public void onNext(final TestAckable ackable) {
        if(trace) {
            System.out.println("get: " + ackable.getMsg());
        }
        ackable.observe().subscribe(new Action1<String>() {
            @Override
            public void call(String msg) {
                // finished one, request one more
                request(1);
            }
        });
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            // ignore
        }
        if(trace) {
            System.out.println("ack: " + ackable.getMsg());
        }
        ackable.ack();
        latch.countDown();
    }
}
