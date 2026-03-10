package mj.mysqlagent.slowlog;

import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SlowLogTailer implements Runnable {

    private final Path file;
    private final long pollIntervalMillis;
    private final EntryHandler handler;
    private volatile boolean running = true;

    public interface EntryHandler {
        void onEntry(List<String> lines) throws Exception;
    }

    public SlowLogTailer(Path file, long pollIntervalMillis, EntryHandler handler) {
        this.file = file;
        this.pollIntervalMillis = pollIntervalMillis;
        this.handler = handler;
    }

    public void stop() {
        running = false;
    }

    @Override
    public void run() {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            long pointer = raf.length();
            List<String> buffer = new ArrayList<>();

            while (running) {
                long len = raf.length();

                if (len < pointer) {
                    pointer = len;
                    buffer.clear();
                }

                if (len > pointer) {
                    raf.seek(pointer);
                    String rawLine;

                    while ((rawLine = raf.readLine()) != null) {
                        String line = new String(
                                rawLine.getBytes(StandardCharsets.ISO_8859_1),
                                StandardCharsets.UTF_8
                        );

                        if (line.startsWith("# Time:") && !buffer.isEmpty()) {
                            handler.onEntry(new ArrayList<>(buffer));
                            buffer.clear();
                        }

                        buffer.add(line);
                    }

                    pointer = raf.getFilePointer();
                }

                Thread.sleep(pollIntervalMillis);
            }

            if (!buffer.isEmpty()) {
                handler.onEntry(new ArrayList<>(buffer));
            }

        } catch (Exception ignored) {
        }
    }
}
