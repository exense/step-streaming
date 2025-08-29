package step.streaming.common;

import java.io.IOException;

public class QuotaExceededException extends IOException {
    public QuotaExceededException(String message) {
        super(message);
    }
}
