package ru.insoftjla.locker;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.logging.Logger;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import ru.insoftjla.locker.annotation.ScheduledLock;
import ru.insoftjla.locker.dao.SchedulerLockDao;

@Aspect
public class SchedulerLock {

    @Autowired
    private Environment env;

    private Duration duration;

    private static final Logger log = Logger.getLogger(SchedulerLockDao.class.getName());

    private final SchedulerLockDao schedulerLockDao;

    public SchedulerLock(SchedulerLockDao schedulerLockDao) {
        this.schedulerLockDao = schedulerLockDao;
    }

    @Around("@annotation(ru.insoftjla.locker.annotation.ScheduledLock)")
    public Object scheduledJobInvoke(ProceedingJoinPoint joinPoint) throws Throwable {
        ScheduledLock annotation = ((MethodSignature) joinPoint.getSignature()).getMethod().getAnnotation(ScheduledLock.class);
        String lockName = annotation.name();

        if (duration == null) {
            String lockedAtFor = env.getProperty(annotation.duration(), annotation.duration());
            duration = Duration.parse("PT" + lockedAtFor);
        }

        LocalDateTime locketAt = LocalDateTime.now().plus(duration);

        if (!schedulerLockDao.doLock(lockName, locketAt)) {
            log.info("The job is skipped because " + lockName + " is locked");
            return null;
        }
        return joinPoint.proceed();
    }

}
