package step.streaming.data;

import java.util.concurrent.CompletableFuture;

/**
 * Class for externally signalling that input is finished.
 * @see EndOfInputRequiringInputStream
 */
public class EndOfInputSignal extends CompletableFuture<Void> {
}
