package rx.backpressure;

import java.lang.InterruptedException;import java.lang.Iterable;import java.lang.Override;import java.lang.String;import java.lang.Thread;import java.lang.UnsupportedOperationException;import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class TestIterable implements Iterable<TestAckable> {
    private final int id;
    private final long delay;
    private final AtomicLong emitCount = new AtomicLong(0);

    public TestIterable(final int id, final long delay) {
        this.id = id;
        this.delay = delay;
    }

    @Override
    public Iterator<TestAckable> iterator() {
        return new Iterator<TestAckable>() {
            // kafka is an infinite stream and always has item
            @Override
            public boolean hasNext() {
                return true;
            }

            // this can block
            @Override
            public TestAckable next() {
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    //e.printStackTrace();
                }
                String msg = String.format("stream = %d, msg = %d", id, emitCount.incrementAndGet());
                return new TestAckable(msg);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("remove not supported");
            }
        };
    }
}