package rx.backpressure;

import rx.Observable;
import rx.subjects.ReplaySubject;import java.lang.String;

class TestAckable {
    private final ReplaySubject<String> subject = ReplaySubject.create();
    private final String msg;

    public TestAckable(String msg) {
        this.msg = msg;
    }

    public void ack() {
        // doesn't matter what value
        subject.onNext(msg);
        subject.onCompleted();
    }

    public Observable<String> observe() {
        return subject.take(1);
    }

    public String getMsg() {
        return msg;
    }
}
