package ru.insoftjla.locker.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
/**
 * Annotation used to mark scheduled methods that require a distributed lock.
 * The annotated method will only be executed if the lock can be acquired.
 */
public @interface ScheduledLock {

    /**
     * Property key or plain value defining the duration for which the lock
     * should be held.
     *
     * @return lock duration in {@link java.time.Duration#parse(CharSequence)}
     *         format without the leading {@code PT}
     */
    String duration();

    /**
     * Unique name of the lock shared across all application instances.
     *
     * @return the lock name
     */
    String name();
}
