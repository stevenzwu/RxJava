package rx.backpressure;

import rx.Observable;
import rx.Producer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.schedulers.Schedulers;
import rx.subjects.ReplaySubject;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ReactivePullTest {

    public static void main(String... args) {

        Observable
                .create(new Observable.OnSubscribe<Ackable>() {
                    @Override
                    public void call(Subscriber<? super Ackable> subscriber) {
                        PullProducer p = new PullProducer(subscriber);
                        subscriber.setProducer(p);
                        System.out.println("init req: " + p.requested.get());
                    }
                })
                .observeOn(Schedulers.io())
                .subscribe(new Subscriber<Ackable>() {
                    @Override
                    public void onStart() {
                        // initial capacity
                        request(10);
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
                    public void onNext(final Ackable ackable) {
                        System.out.println("get: " + ackable.getMsg());
                        ackable.observe().subscribe(new Action1<Long>() {
                            @Override
                            public void call(Long msg) {
                                // finished one, request one more
                                request(1);
                            }
                        });
                        Observable.timer(1000, TimeUnit.MILLISECONDS)
                                .doOnCompleted(new Action0() {
                                    @Override
                                    public void call() {
                                        ackable.ack();
                                    }
                                });
                    }
                });

        try {
            Thread.sleep(1000 * 1000 * 1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class PullProducer implements Producer {

        private final Subscriber<? super Ackable> s;

        private final AtomicLong requested = new AtomicLong();

        private final AtomicLong id = new AtomicLong();

        public PullProducer(final Subscriber<? super Ackable> s) {
            this.s = s;
        }

        @Override
        public void request(long n) {
            long c = requested.getAndAdd(n);
            if (c == 0) {
                while (true) {
                    long r = requested.get();
                    long numToEmit = r;
                    while (--numToEmit >= 0) {
                        if (s.isUnsubscribed()) {
                            return;
                        }
                        s.onNext(new Ackable(id.incrementAndGet()));
                    }
                    requested.addAndGet(-r);
                }
            }
        }
    }

}