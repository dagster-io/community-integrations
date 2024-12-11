package pipes.writers;

import pipes.DagsterPipesException;

import java.io.StringWriter;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class PipesBlobStoreMessageWriterChannel implements PipesMessageWriterChannel {

    private final float interval;
    private final Queue<PipesMessage> buffer;
    private final AtomicInteger counter;
    private final Lock lock;
    private Thread uploadThread;
    private volatile boolean shouldClose;

    public PipesBlobStoreMessageWriterChannel(float interval) {
        this.interval = interval;
        this.buffer = new ConcurrentLinkedQueue<>();
        this.counter = new AtomicInteger(1);
        this.lock = new ReentrantLock();
    }

    /**
     * Adds a message to the buffer.
     */
    @Override
    public void writeMessage(PipesMessage message) {
        buffer.add(message);
    }

    /**
     * Flushes messages from the buffer.
     */
    private Queue<PipesMessage> flushMessages() {
        Queue<PipesMessage> messages = new ConcurrentLinkedQueue<>();
        lock.lock();
        try {
            while (!buffer.isEmpty()) {
                messages.add(buffer.poll());
            }
        } finally {
            lock.unlock();
        }
        return messages;
    }

    /**
     * Uploads a chunk of messages.
     */
    protected abstract void uploadMessagesChunk(StringWriter payload, int index);

    /**
     * Starts a buffered upload loop in a separate thread.
     */
    public void startBufferedUploadLoop() {
        this.uploadThread = new Thread(this::uploadLoop);
        this.uploadThread.setDaemon(true);
        this.uploadThread.start();
    }

    @Override
    public void close() throws DagsterPipesException {
        this.shouldClose = true;
        try {
            this.uploadThread.join();
        } catch (InterruptedException interruptedException) {
            throw new DagsterPipesException("Failed to join thread!", interruptedException);
        }
    }

    /**
     * Handles the upload loop logic.
     */
    private void uploadLoop() {
        LocalDateTime startOrLastUpload = LocalDateTime.now();
        StringWriter payload;

        while (!shouldClose) {
            try {
                LocalDateTime now = LocalDateTime.now();
                boolean shouldUpload = Duration.between(startOrLastUpload, now).getSeconds() > interval;

                if (!buffer.isEmpty() && shouldUpload) {
                    Queue<PipesMessage> messagesToUpload = flushMessages();
                    payload = new StringWriter();
                    for (PipesMessage message: messagesToUpload) {
                        payload.write(message.toString());
                        payload.write("\n");
                    }

                    if (payload.getBuffer().length() > 0) {
                        uploadMessagesChunk(payload, counter.getAndIncrement());
                        startOrLastUpload = now;
                    }
                }
                Thread.sleep((long) this.interval);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        Queue<PipesMessage> messagesToUpload = flushMessages();
        payload = new StringWriter();
        for (PipesMessage message: messagesToUpload) {
            payload.write(message.toString());
            payload.write("\n");
        }

        if (payload.getBuffer().length() > 0) {
            uploadMessagesChunk(payload, counter.getAndIncrement());
        }
    }
}
