package org.sagebionetworks.bridge.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.sagebionetworks.bridge.dao.DistributedLockDao;
import org.sagebionetworks.bridge.dao.ScheduleDao;
import org.sagebionetworks.bridge.dao.SchedulePlanDao;
import org.sagebionetworks.bridge.events.SchedulePlanCreatedEvent;
import org.sagebionetworks.bridge.events.SchedulePlanDeletedEvent;
import org.sagebionetworks.bridge.events.SchedulePlanUpdatedEvent;
import org.sagebionetworks.bridge.events.UserEnrolledEvent;
import org.sagebionetworks.bridge.events.UserUnenrolledEvent;
import org.sagebionetworks.bridge.exceptions.ConcurrentModificationException;
import org.sagebionetworks.bridge.models.BridgeEntity;
import org.sagebionetworks.bridge.models.Study;
import org.sagebionetworks.bridge.models.User;
import org.sagebionetworks.bridge.models.schedules.Schedule;
import org.sagebionetworks.bridge.models.schedules.SchedulePlan;
import org.sagebionetworks.bridge.stormpath.StormpathFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEvent;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.stormpath.sdk.account.Account;
import com.stormpath.sdk.account.AccountList;
import com.stormpath.sdk.application.Application;
import com.stormpath.sdk.client.Client;

public class ScheduleChangeWorker implements Callable<Boolean> {
    
    private static Logger logger = LoggerFactory.getLogger(ScheduleChangeWorker.class);
    
    private static final Random rand = new Random();
    
    private interface Command {
        void execute();
    }

    private ApplicationEvent event;
    private Client stormpathClient;
    private DistributedLockDao lockDao;
    private ScheduleDao scheduleDao;
    private SchedulePlanDao schedulePlanDao;
    private StudyService studyService;
   
    public void setStormpathClient(Client stormpathClient) {
        this.stormpathClient = stormpathClient;
    }
    public void setDistributedLockDao(DistributedLockDao lockDao) {
        this.lockDao = lockDao;
    }
    public void setScheduleDao(ScheduleDao scheduleDao) {
        this.scheduleDao = scheduleDao;
    }
    public void setSchedulePlanDao(SchedulePlanDao schedulePlanDao) {
        this.schedulePlanDao = schedulePlanDao;
    }
    public void setStudyService(StudyService studyService) {
        this.studyService = studyService;
    }
    public void setApplicationEvent(ApplicationEvent event) {
        this.event = event;
    }
    
    @Override
    public Boolean call() throws Exception {
        try {
            if (event instanceof SchedulePlanCreatedEvent) {
                schedulePlanCreated((SchedulePlanCreatedEvent)event);
            } 
            else if (event instanceof SchedulePlanUpdatedEvent) {
                schedulePlanUpdated((SchedulePlanUpdatedEvent)event);
            } 
            else if (event instanceof SchedulePlanDeletedEvent) {
                schedulePlanDeleted((SchedulePlanDeletedEvent)event);
            } 
            else if (event instanceof UserEnrolledEvent) {
                userEnrolled((UserEnrolledEvent)event);
            } 
            else if (event instanceof UserUnenrolledEvent) {
                userUnenrolled((UserUnenrolledEvent)event);
            }        
        } catch(Throwable throwable) {
            logger.error(throwable.getMessage(), throwable);
            Throwables.propagateIfInstanceOf(throwable,  InterruptedException.class);
            return false;
        }
        return true;
    }

    private void schedulePlanCreated(SchedulePlanCreatedEvent event) throws InterruptedException {
        logger.info("EVENT: Schedule plan "+event.getSchedulePlan().getGuid()+" created");
        
        final SchedulePlan plan = event.getSchedulePlan();
        final Study study = studyService.getStudyByKey(plan.getStudyKey());
        final ArrayList<User> users = getStudyUsers();
        final List<Schedule> schedules = plan.getStrategy().scheduleExistingUsers(study, users);
        setSchedulePlan(schedules, plan);
        
        runWithLock(plan.getClass(), plan.getGuid(), new Command() {
            public void execute() {
                scheduleDao.createSchedules(schedules);
            }
        });
    }
    private void schedulePlanDeleted(SchedulePlanDeletedEvent event) throws InterruptedException {
        logger.info("EVENT: Schedule plan "+event.getSchedulePlan().getGuid()+" deleted");
        
        final SchedulePlan plan = event.getSchedulePlan();
        
        runWithLock(plan.getClass(), plan.getGuid(), new Command() {
            public void execute() {
                scheduleDao.deleteSchedules(plan);
            }
        });
    }
    private void schedulePlanUpdated(SchedulePlanUpdatedEvent event) throws InterruptedException {
        logger.info("EVENT: Schedule plan "+event.getSchedulePlan().getGuid()+" updated");
        
        final SchedulePlan plan = event.getSchedulePlan();
        final Study study = studyService.getStudyByKey(plan.getStudyKey());
        final ArrayList<User> users = getStudyUsers();
        final List<Schedule> schedules = plan.getStrategy().scheduleExistingUsers(study, users);
        setSchedulePlan(schedules, plan);
        
        runWithLock(plan.getClass(), plan.getGuid(), new Command() {
            public void execute() {
                scheduleDao.deleteSchedules(plan);
                scheduleDao.createSchedules(schedules);
            }
        });
    }
    private void userEnrolled(UserEnrolledEvent event) throws InterruptedException {
        logger.info("EVENT: User " + event.getUser().getId() + " enrolled in study " + event.getStudy().getKey());
        
        final Study study = event.getStudy();
        final User user = event.getUser();
        final List<SchedulePlan> plans = schedulePlanDao.getSchedulePlans(event.getStudy());
        final List<Schedule> schedules = Lists.newArrayListWithCapacity(plans.size()); 
        for (SchedulePlan plan : plans) {
            Schedule schedule = plan.getStrategy().scheduleNewUser(study, user);
            if (schedule != null) {
                schedule.setSchedulePlanGuid(plan.getGuid());
                schedules.add(schedule);
            }
        }
        runWithLock(user.getClass(), user.getId(), new Command() {
            public void execute() {
                scheduleDao.createSchedules(schedules);
            }
        });
    }
    private void userUnenrolled(UserUnenrolledEvent event) throws InterruptedException {
        logger.info("EVENT: User " + event.getUser().getId() + " withdrawn from study " + event.getStudy().getKey());

        final Study study = event.getStudy();
        final User user = event.getUser();
        runWithLock(user.getClass(), user.getId(), new Command() {
            public void execute() {
                scheduleDao.deleteSchedules(study, user);
            }
        });
    }
    
    private void runWithLock(Class<? extends BridgeEntity> clazz, String id, Command command) throws InterruptedException {
        String lockId = null;
        while (true) {
            try {
                lockId = lockDao.createLock(clazz, id);
                command.execute();
                return;
            } catch(ConcurrentModificationException e) {
                logger.info("Lock held, waiting to retry");
                Thread.sleep(300 + rand.nextInt(400));
            } catch(Throwable t) {
                logger.info(t.getMessage());
            } finally {
                lockDao.releaseLock(clazz, id, lockId);
            }
        }
    }
    
    /*
    private void runWithLock(Study study, Command command) throws InterruptedException {
        String lockId = null;
        while (true) {
            try {
                lockId = lockDao.createLock(Study.class, study.getKey());
                command.execute();
                return;
            } catch(ConcurrentModificationException e) {
                logger.info("Lock held, waiting to retry");
                Thread.sleep(300 + rand.nextInt(400));
            } catch(Throwable t) {
                logger.info(t.getMessage());
            } finally {
                lockDao.releaseLock(Study.class, study.getKey(), lockId);
            }
        }
    }
    */
    
    private void setSchedulePlan(List<Schedule> schedules, SchedulePlan plan) {
        for (Schedule schedule : schedules) {
            schedule.setSchedulePlanGuid(plan.getGuid());
        }
    }
    
    // TODO: Go back to the user consents to verify the user is in the study. 
    // This will be slow, but accurate, if/until/when we have better.
    private ArrayList<User> getStudyUsers() {
        ArrayList<User> users = Lists.newArrayList();
        Application application = StormpathFactory.createStormpathApplication(stormpathClient);
        AccountList accounts = application.getAccounts(); 
        for (Account account : accounts) {
            users.add(new User(account));
        }
        return users;
    }
    
}
