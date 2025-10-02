package step.streaming.common;

/**
 * This class indicates errors specifically related to quota enforcement.
 */
public class QuotaExceededException extends Exception {
    public QuotaExceededException(String message) {
        super(message);
    }
}
