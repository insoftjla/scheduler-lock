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

@Aspect
public class SchedulerLock {

    private Properties properties = new Properties();

    private final Map<String, Duration> durations = new HashMap<>();

    private static final Logger log = Logger.getLogger(SchedulerLockDao.class.getName());

    private final SchedulerLockDao schedulerLockDao;

    public SchedulerLock(SchedulerLockDao schedulerLockDao) {
        this.schedulerLockDao = schedulerLockDao;
    }

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

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

}
