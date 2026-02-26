package step.streaming.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public final class ExceptionsUtil {
    private ExceptionsUtil() {
    }

    /**
     * Returns {@code e} if it already is an instance of {@code targetType},
     * otherwise creates a new instance of {@code targetType} wrapping {@code e}.
     * <p>
     * Preferred constructors (in order):
     * 1) (String, Throwable)
     * 2) (Throwable)
     * 3) (String)  + initCause(e)
     * 4) ()        + initCause(e)
     */
    public static <T extends Exception> T as(Exception e, Class<T> targetType) {
        if (targetType.isInstance(e)) {
            return targetType.cast(e);
        }

        // 1) (String, Throwable)
        T inst = tryNew(targetType, new Class<?>[]{String.class, Throwable.class}, new Object[]{e.getMessage(), e});
        if (inst != null) return inst;

        // 2) (Throwable)
        inst = tryNew(targetType, new Class<?>[]{Throwable.class}, new Object[]{e});
        if (inst != null) return inst;

        // 3) (String) + initCause
        inst = tryNew(targetType, new Class<?>[]{String.class}, new Object[]{String.valueOf(e)});
        if (inst != null) {
            safeInitCause(inst, e);
            return inst;
        }

        // 4) () + initCause
        inst = tryNew(targetType, new Class<?>[0], new Object[0]);
        if (inst != null) {
            safeInitCause(inst, e);
            return inst;
        }

        throw new IllegalArgumentException(
            "Cannot construct " + targetType.getName() + " to wrap " + e.getClass().getName(), e);
    }

    private static <T extends Exception> T tryNew(Class<T> type, Class<?>[] sig, Object[] args) {
        try {
            Constructor<T> c = type.getDeclaredConstructor(sig);
            if (!c.canAccess(null)) c.setAccessible(true);
            return c.newInstance(args);
        } catch (NoSuchMethodException ignored) {
            return null;
        } catch (InvocationTargetException | InstantiationException | IllegalAccessException reflectEx) {
            // Constructor exists but failed; treat as unusable and keep searching.
            return null;
        }
    }

    private static void safeInitCause(Throwable target, Throwable cause) {
        try {
            target.initCause(cause);
        } catch (IllegalStateException | IllegalArgumentException ignored) {
            // cause already set or not allowed; best effort.
        }
    }
}
