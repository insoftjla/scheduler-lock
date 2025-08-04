package ru.insoftjla.locker;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import ru.insoftjla.locker.annotation.ScheduledLock;
import ru.insoftjla.locker.dao.SchedulerLockDao;

/**
 * Aspect that ensures scheduled jobs are executed only when the corresponding
 * lock is available. The aspect intercepts methods annotated with
 * {@link ru.insoftjla.locker.annotation.ScheduledLock} and attempts to acquire a
 * lock before proceeding with the invocation.
 */
@Aspect
public class SchedulerLock {

    private Properties properties = new Properties();

    private final Map<String, Duration> durations = new HashMap<>();

    private static final Logger log = Logger.getLogger(SchedulerLockDao.class.getName());

    private final SchedulerLockDao schedulerLockDao;

    /**
     * Creates a new instance of the aspect using the provided DAO for persisting
     * lock information.
     *
     * @param schedulerLockDao DAO used to manage lock state in the storage
     */
    public SchedulerLock(SchedulerLockDao schedulerLockDao) {
        this.schedulerLockDao = schedulerLockDao;
    }

    /**
     * Intercepts scheduled methods, attempting to acquire the lock specified in
     * the {@link ScheduledLock} annotation. If the lock cannot be acquired the
     * method invocation is skipped.
     *
     * @param joinPoint intercepted join point representing the method execution
     * @return result of the intercepted method or {@code null} when execution is
     *         skipped
     * @throws Throwable if the intercepted method throws any exception
     */
    @Around("@annotation(ru.insoftjla.locker.annotation.ScheduledLock)")
    public Object scheduledJobInvoke(ProceedingJoinPoint joinPoint) throws Throwable {
        ScheduledLock annotation = ((MethodSignature) joinPoint.getSignature()).getMethod().getAnnotation(ScheduledLock.class);
        String lockName = annotation.name();

        if (!durations.containsKey(lockName)) {
            String lockedAtFor = properties.getProperty(annotation.duration(), annotation.duration());
            durations.put(lockName, Duration.parse("PT" + lockedAtFor));
        }

        LocalDateTime locketAt = LocalDateTime.now().plus(durations.get(lockName));

        if (!schedulerLockDao.doLock(lockName, locketAt)) {
            log.info("The job is skipped because " + lockName + " is locked");
            return null;
        }
        return joinPoint.proceed();
    }

    /**
     * Sets external properties used to resolve lock durations.
     *
     * @param properties configuration properties with duration definitions
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}
