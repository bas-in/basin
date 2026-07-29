package io.basin.sdk;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handle returned by {@link RealtimeClient#subscribe}.
 *
 * <p>Call {@link #unsubscribe()} to stop receiving events and clean up the
 * background driver thread.
 */
public final class ChannelHandle {

    private final String table;
    private final BlockingQueue<Object> queue;
    private final Thread driverThread;
    private final AtomicBoolean stopped;
    private final RealtimeClient realtime;

    ChannelHandle(String table, BlockingQueue<Object> queue, Thread driverThread,
                  AtomicBoolean stopped, RealtimeClient realtime) {
        this.table = table;
        this.queue = queue;
        this.driverThread = driverThread;
        this.stopped = stopped;
        this.realtime = realtime;
    }

    /** Table this handle is subscribed to. */
    public String table() {
        return table;
    }

    /**
     * Stop receiving events, cancel the driver thread, and remove this
     * subscription from the realtime client.
     */
    public void unsubscribe() {
        stopped.set(true);
        queue.offer(RealtimeClient.SENTINEL);
        driverThread.interrupt();
        realtime.removeQueue(table, queue);
    }
}
