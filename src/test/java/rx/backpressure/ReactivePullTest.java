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
                        subscriber.setProducer(new PullProducer(subscriber));
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
                        System.out.println("get: " + ackable.msg);
                        ackable.observe().subscribe(new Action1<Integer>() {
                            @Override
                            public void call(Integer integer) {
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
                        s.onNext(new Ackable(Long.toString(id.incrementAndGet())));
                    }
                    requested.addAndGet(-r);
                }
            }
        }
    }

    private static class Ackable {
        private final ReplaySubject<Integer> subject = ReplaySubject.create();
        private final String msg;
        public Ackable(String msg) {
            this.msg = msg;
        }

        public void ack() {
            System.out.println("ack: " + msg);
            // doesn't matter what value
            subject.onNext(1);
            subject.onCompleted();
        }

        public Observable<Integer> observe() {
            return subject.take(1);
        }
    }
}