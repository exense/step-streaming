package step.streaming.server;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

public interface LinebreakIndex extends Closeable {
    long getTotalEntries();

    long addLinebreakPosition(Long linebreakPosition) throws IOException;

    long getLinebreakPosition(long index) throws IOException;

    List<Long> getLinebreakPositions(long startIndex, long count) throws IOException;
}
