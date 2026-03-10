package mj.mysqlagent.queue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class FileQueue {
    private final Path dir;
    private final long maxSegmentBytes;
    private Path currentSegment;

    public FileQueue(Path dir, long maxSegmentBytes) throws IOException {
        this.dir = dir;
        this.maxSegmentBytes = maxSegmentBytes;
        Files.createDirectories(dir);
        rollIfNeeded(true);
    }

    public synchronized void appendLine(String ndjsonLine) throws IOException {
        rollIfNeeded(false);
        Files.writeString(
                currentSegment,
                ndjsonLine + "\n",
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
    }

    public synchronized Optional<Segment> peekOldestSegment() throws IOException {
        try (var stream = Files.list(dir)) {
            List<Path> segs = stream
                    .filter(p -> {
                        String name = p.getFileName().toString();
                        return name.startsWith("seg-") && name.endsWith(".ndjson");
                    })
                    .sorted(Comparator.naturalOrder())
                    .collect(Collectors.toList());

            if (segs.isEmpty()) {
                return Optional.empty();
            }

            for (Path p : segs) {
                if (!p.equals(currentSegment)) {
                    return Optional.of(new Segment(p, Files.readAllBytes(p)));
                }
            }

            return Optional.empty();
        }
    }

    public synchronized void ack(Path segmentPath) throws IOException {
        Files.deleteIfExists(segmentPath);
    }

    public synchronized void forceRoll() throws IOException {
        currentSegment = dir.resolve(nextSegmentName());
        if (!Files.exists(currentSegment)) {
            Files.createFile(currentSegment);
        }
    }

    private void rollIfNeeded(boolean forceIfMissing) throws IOException {
        if (currentSegment == null && forceIfMissing) {
            currentSegment = dir.resolve(nextSegmentName());
            if (!Files.exists(currentSegment)) {
                Files.createFile(currentSegment);
            }
            return;
        }

        if (currentSegment == null) {
            return;
        }

        long size = Files.size(currentSegment);
        if (size >= maxSegmentBytes) {
            currentSegment = dir.resolve(nextSegmentName());
            if (!Files.exists(currentSegment)) {
                Files.createFile(currentSegment);
            }
        }
    }

    private String nextSegmentName() throws IOException {
        int max = 0;
        try (var stream = Files.list(dir)) {
            for (Path p : stream.toList()) {
                String name = p.getFileName().toString();
                if (name.startsWith("seg-") && name.endsWith(".ndjson")) {
                    String n = name.substring(4, 10);
                    max = Math.max(max, Integer.parseInt(n));
                }
            }
        }
        return String.format("seg-%06d.ndjson", max + 1);
    }

    public record Segment(Path path, byte[] bytes) {
    }
}