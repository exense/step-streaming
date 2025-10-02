package step.streaming.server;

import java.io.IOException;
import java.util.stream.Stream;

public interface LinebreakIndex {
    long getTotalEntries();

    long addLinebreakPosition(Long linebreakPosition) throws IOException;

    long getLinebreakPosition(long index) throws IOException;

    Stream<Long> getLinebreakPositions(long startIndex, long count) throws IOException;
}
