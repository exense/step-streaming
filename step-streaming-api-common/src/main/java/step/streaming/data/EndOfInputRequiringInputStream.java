package step.streaming.data;

import java.io.InputStream;

/**
 * An {@link InputStream} that relies on an explicit external signal to determine end-of-input (EOF).
 * <p>
 * Implementations typically wrap or delegate to one or more underlying {@link InputStream}s to provide data.
 * However, they delay signaling EOF until the associated {@link EndOfInputSignal} confirms that the input
 * has been fully provided.
 * <p>
 * This is useful for streaming scenarios where the data source may still be growing (e.g. live uploads).
 */
public abstract class EndOfInputRequiringInputStream extends InputStream {
    /**
     * Returns the external signal used to determine when the input has completed.
     *
     * @return the {@link EndOfInputSignal} associated with this stream
     */
    abstract EndOfInputSignal getEndOfInputSignal();
}
