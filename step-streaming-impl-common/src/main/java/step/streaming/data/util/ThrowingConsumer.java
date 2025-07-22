package step.streaming.data.util;

@FunctionalInterface
public interface ThrowingConsumer<T> {
    void accept(T value) throws Exception;
}
