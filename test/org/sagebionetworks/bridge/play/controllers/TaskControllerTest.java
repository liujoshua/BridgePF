package org.sagebionetworks.bridge.play.controllers;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.sagebionetworks.bridge.TestConstants.TEST_STUDY;

import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.BridgeUtils;
import org.sagebionetworks.bridge.TestConstants;
import org.sagebionetworks.bridge.TestUtils;
import org.sagebionetworks.bridge.dynamodb.DynamoTask;
import org.sagebionetworks.bridge.exceptions.NotAuthenticatedException;
import org.sagebionetworks.bridge.json.BridgeObjectMapper;
import org.sagebionetworks.bridge.models.accounts.User;
import org.sagebionetworks.bridge.models.accounts.UserSession;
import org.sagebionetworks.bridge.models.schedules.ScheduleContext;
import org.sagebionetworks.bridge.models.schedules.Task;
import org.sagebionetworks.bridge.play.controllers.TaskController;
import org.sagebionetworks.bridge.services.TaskService;

import play.mvc.Http;

import com.google.common.collect.Lists;

public class TaskControllerTest {

    private TaskService taskService;
    
    private TaskController controller;
    
    @Before
    public void before() throws Exception {
        ScheduleContext scheduleContext = new ScheduleContext(TEST_STUDY, DateTimeZone.UTC, null, null, null, BridgeUtils.generateGuid());
        
        DynamoTask task = new DynamoTask();
        task.setGuid(BridgeUtils.generateGuid());
        task.setLocalScheduledOn(LocalDateTime.now(DateTimeZone.UTC).minusDays(1));
        task.setActivity(TestConstants.TEST_ACTIVITY);
        task.setRunKey(BridgeUtils.generateTaskRunKey(task, scheduleContext));
        List<Task> list = Lists.newArrayList(task);
        
        String json = BridgeObjectMapper.get().writeValueAsString(list);
        Http.Context context = TestUtils.mockPlayContextWithJson(json);
        Http.Context.current.set(context);
        
        UserSession session = new UserSession();
        User user = new User();
        user.setHealthCode("BBB");
        session.setUser(user);
        
        taskService = mock(TaskService.class);
        when(taskService.getTasks(any(User.class), any(ScheduleContext.class))).thenReturn(list);
        
        controller = spy(new TaskController());
        controller.setTaskService(taskService);
        doReturn(session).when(controller).getAuthenticatedAndConsentedSession();
    }
    
    @Test
    public void getTasks() throws Exception {
        controller.getTasks(DateTime.now().toString(), null, null);
        verify(taskService).getTasks(any(User.class), any(ScheduleContext.class));
        verifyNoMoreInteractions(taskService);
    }
    
    @SuppressWarnings("unchecked")
    @Test
    public void updateTasks() throws Exception {
        controller.updateTasks();
        verify(taskService).updateTasks(anyString(), any(List.class));
        verifyNoMoreInteractions(taskService);
    }
    
    @Test(expected = NotAuthenticatedException.class)
    public void mustBeAuthenticated() throws Exception {
        controller = new TaskController();
        controller.getTasks(DateTime.now().toString(), null, null);
    }
    
}
