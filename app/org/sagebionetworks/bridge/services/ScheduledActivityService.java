package org.sagebionetworks.bridge.services;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.sagebionetworks.bridge.BridgeConstants.API_MAXIMUM_PAGE_SIZE;
import static org.sagebionetworks.bridge.BridgeConstants.API_MINIMUM_PAGE_SIZE;
import static org.sagebionetworks.bridge.BridgeConstants.CLIENT_DATA_MAX_BYTES;
import static org.sagebionetworks.bridge.models.schedules.ScheduledActivityStatus.UPDATABLE_STATUSES;
import static org.sagebionetworks.bridge.util.BridgeCollectors.toImmutableList;
import static org.sagebionetworks.bridge.validators.ScheduleContextValidator.MAX_DATE_RANGE_IN_DAYS;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.dao.ScheduledActivityDao;
import org.sagebionetworks.bridge.exceptions.BadRequestException;
import org.sagebionetworks.bridge.models.CriteriaContext;
import org.sagebionetworks.bridge.models.schedules.Activity;
import org.sagebionetworks.bridge.models.schedules.ActivityType;
import org.sagebionetworks.bridge.models.schedules.CompoundActivity;
import org.sagebionetworks.bridge.models.schedules.CompoundActivityDefinition;
import org.sagebionetworks.bridge.models.schedules.Schedule;
import org.sagebionetworks.bridge.models.schedules.ScheduleContext;
import org.sagebionetworks.bridge.models.schedules.SchedulePlan;
import org.sagebionetworks.bridge.models.schedules.ScheduledActivity;
import org.sagebionetworks.bridge.models.schedules.ScheduledActivityList;
import org.sagebionetworks.bridge.models.schedules.ScheduledActivityStatus;
import org.sagebionetworks.bridge.models.schedules.SchemaReference;
import org.sagebionetworks.bridge.models.schedules.SurveyReference;
import org.sagebionetworks.bridge.models.schedules.TaskReference;
import org.sagebionetworks.bridge.models.surveys.Survey;
import org.sagebionetworks.bridge.models.upload.UploadSchema;
import org.sagebionetworks.bridge.validators.ScheduleContextValidator;
import org.sagebionetworks.bridge.validators.Validate;

@Component
public class ScheduledActivityService {

    static final Predicate<ScheduledActivity> V3_FILTER = activity -> {
        return ScheduledActivityStatus.VISIBLE_STATUSES.contains(activity.getStatus());
    };
    
    static final Predicate<ScheduledActivity> V4_FILTER = activity -> {
        return activity.getStatus() != ScheduledActivityStatus.DELETED;
    };
    
    private static final String PAGE_SIZE_ERROR = "pageSize must be from " + API_MINIMUM_PAGE_SIZE + "-"
            + API_MAXIMUM_PAGE_SIZE + " records";

    private static final String EITHER_BOTH_DATES_OR_NEITHER = "Only one date of a date range provided (both scheduledOnStart and scheduledOnEnd required)";

    private static final String AMBIGUOUS_TIMEZONE_ERROR = "scheduledOnStart and scheduledOnEnd must be in the same time zone";

    private static final String ENROLLMENT = "enrollment";

    private static final ScheduleContextValidator VALIDATOR = new ScheduleContextValidator();

    private ScheduledActivityDao activityDao;

    private ActivityEventService activityEventService;

    private CompoundActivityDefinitionService compoundActivityDefinitionService;

    private SchedulePlanService schedulePlanService;

    private UploadSchemaService schemaService;

    private SurveyService surveyService;

    @Autowired
    final void setScheduledActivityDao(ScheduledActivityDao activityDao) {
        this.activityDao = activityDao;
    }
    @Autowired
    final void setActivityEventService(ActivityEventService activityEventService) {
        this.activityEventService = activityEventService;
    }

    /**
     * Compound Activity Definition service, used to resolve compound activities (references) in activity schedules.
     */
    @Autowired
    final void setCompoundActivityDefinitionService(
            CompoundActivityDefinitionService compoundActivityDefinitionService) {
        this.compoundActivityDefinitionService = compoundActivityDefinitionService;
    }

    @Autowired
    final void setSchedulePlanService(SchedulePlanService schedulePlanService) {
        this.schedulePlanService = schedulePlanService;
    }

    /** Schema service, used to resolve schema revision numbers for schema references in activities. */
    @Autowired
    final void setSchemaService(UploadSchemaService schemaService) {
        this.schemaService = schemaService;
    }

    @Autowired
    final void setSurveyService(SurveyService surveyService) {
        this.surveyService = surveyService;
    }

    public ScheduledActivityList getActivityHistory(String healthCode,
            String activityGuid, DateTime scheduledOnStart, DateTime scheduledOnEnd, String offsetBy,
            int pageSize) {
        checkArgument(isNotBlank(healthCode));
        checkArgument(isNotBlank(activityGuid));

        if (pageSize < API_MINIMUM_PAGE_SIZE || pageSize > API_MAXIMUM_PAGE_SIZE) {
            throw new BadRequestException(PAGE_SIZE_ERROR);
        }
        // If nothing is provided, we will default to two weeks, going max days into future.
        if (scheduledOnStart == null && scheduledOnEnd == null) {
            DateTime now = getDateTime();
            scheduledOnEnd = now.plusDays(MAX_DATE_RANGE_IN_DAYS/2);
            scheduledOnStart = now.minusDays(MAX_DATE_RANGE_IN_DAYS/2);
        }
        // But if only one was provided... we don't know what to do with this. Return bad request exception
        if (scheduledOnStart == null || scheduledOnEnd == null) {
            throw new BadRequestException(EITHER_BOTH_DATES_OR_NEITHER);
        }

        DateTimeZone timezone = scheduledOnStart.getZone();
        if (!timezone.equals(scheduledOnEnd.getZone())) {
            throw new BadRequestException(AMBIGUOUS_TIMEZONE_ERROR);
        }

        return activityDao.getActivityHistoryV2(
                healthCode, activityGuid, scheduledOnStart, scheduledOnEnd, timezone, offsetBy, pageSize);
    }
    
    // This needs to be exposed for tests because although we can fix a point of time for tests, we cannot
    // change the time zone in a test without controlling construction of the DateTime instance.
    protected DateTime getDateTime() {
        return DateTime.now();
    }

    public List<ScheduledActivity> getScheduledActivities(ScheduleContext context) {
        checkNotNull(context);
        
        Validate.nonEntityThrowingException(VALIDATOR, context);
        // Add events for scheduling
        Map<String, DateTime> events = createEventsMap(context);
        ScheduleContext updatedContext = new ScheduleContext.Builder().withContext(context).withEvents(events).build();
        
        List<ScheduledActivity> scheduledActivities = scheduleActivitiesForPlans(updatedContext);
        List<ScheduledActivity> dbActivities = activityDao.getActivities(context.getEndsOn().getZone(), scheduledActivities);
        
        // Must be a mutable map because performMerge removes items when found 
        Map<String, ScheduledActivity> dbMap = Maps.newHashMap();
        for (ScheduledActivity dbActivity : dbActivities) {
            dbMap.put(dbActivity.getGuid(), dbActivity);
        }
        
        List<ScheduledActivity> saves = performMerge(scheduledActivities, dbMap);
        activityDao.saveActivities(saves);
        
        return orderActivities(scheduledActivities, V3_FILTER);
    }
    
    public List<ScheduledActivity> getScheduledActivitiesV4(ScheduleContext context) {
        checkNotNull(context);
        
        Validate.nonEntityThrowingException(VALIDATOR, context);
        // Add events for scheduling
        Map<String, DateTime> events = createEventsMap(context);
        ScheduleContext updatedContext = new ScheduleContext.Builder().withContext(context).withEvents(events).build();

        List<ScheduledActivity> scheduledActivities = scheduleActivitiesForPlans(updatedContext);

        // Get all persisted activities within the time frame, not just those found by the scheduler (as in v3).
        Map<String, ScheduledActivity> dbMap = retrieveAllPersistedActivitiesIntoMap(updatedContext, scheduledActivities);
        
        // Compare scheduled and persisted activities, replacing scheduled with persisted where they exist
        List<ScheduledActivity> saves = performMerge(scheduledActivities, dbMap);
        activityDao.saveActivities(saves);
        
        // We've removed all the persisted activities that were found by the scheduler. Those have been saved
        // as necessary. The remaining tasks were scheduled based on user-triggered events that can be updated, 
        // not events like "enrollment" but events like "when this activity is finished." These are now all 
        // added to the activities that will be returned.
        scheduledActivities.addAll(dbMap.values());
        
        return orderActivities(scheduledActivities, V4_FILTER);
    }
    
    protected List<ScheduledActivity> performMerge(List<ScheduledActivity> scheduledActivities,
            Map<String, ScheduledActivity> dbMap) {
        List<ScheduledActivity> saves = Lists.newArrayList();
        for (int i=0; i < scheduledActivities.size(); i++) {
            ScheduledActivity activity = scheduledActivities.get(i);
            ScheduledActivity dbActivity = dbMap.remove(activity.getGuid());
            
            if (dbActivity != null && !UPDATABLE_STATUSES.contains(dbActivity.getStatus())) {
                // Once the activity is in the database and is in a non-updatable state, we should use the one from the
                // database. Otherwise, either (a) it doesn't exist yet and needs to be persisted or (b) the user
                // hasn't interacted with it yet, so we can safely replace it with the newly generated one, which may
                // have updated schemas or surveys.
                //
                // Note that this only works because the scheduled activity guid is actually the schedule plan's
                // activity guid concatenated with scheduled time. So when the scheduler regenerates the scheduled
                // activity, it always has the same guid.
                scheduledActivities.set(i, dbActivity);
            } else if (activity.getStatus() != ScheduledActivityStatus.EXPIRED) {
                saves.add(activity);
            }
        }
        return saves;
    }
    
    private Map<String, ScheduledActivity> retrieveAllPersistedActivitiesIntoMap(ScheduleContext context,
            List<ScheduledActivity> scheduledActivities) {
        
        Set<String> activityGuids = scheduledActivities.stream().map((activity) -> {
            return activity.getGuid().split(":")[0];
        }).collect(Collectors.toSet());
        
        Map<String,ScheduledActivity> dbMap = Maps.newHashMap();
        for (String activityGuid : activityGuids) {
            ScheduledActivityList list = activityDao.getActivityHistoryV2(
                    context.getCriteriaContext().getHealthCode(), activityGuid,
                    context.getStartsOn(), context.getEndsOn(), context.getStartsOn().getZone(), null,
                    API_MAXIMUM_PAGE_SIZE);
            if (list != null) {
                for(ScheduledActivity activity : list.getItems()) {
                    dbMap.put(activity.getGuid(), activity);
                }
            }
        }
        return dbMap;
    }

    public void updateScheduledActivities(String healthCode, List<ScheduledActivity> scheduledActivities) {
        checkArgument(isNotBlank(healthCode));
        checkNotNull(scheduledActivities);

        List<ScheduledActivity> activitiesToSave = Lists.newArrayListWithCapacity(scheduledActivities.size());
        for (int i=0; i < scheduledActivities.size(); i++) {
            ScheduledActivity schActivity = scheduledActivities.get(i);
            if (schActivity == null) {
                throw new BadRequestException("A task in the array is null");
            }
            if (schActivity.getGuid() == null) {
                throw new BadRequestException(String.format("Task #%s has no GUID", i));
            }
            if (byteLength(schActivity.getClientData()) > CLIENT_DATA_MAX_BYTES) {
                throw new BadRequestException("Client data too large ("+CLIENT_DATA_MAX_BYTES+" bytes limit)");
            }
            ScheduledActivity dbActivity = activityDao.getActivity(healthCode, schActivity.getGuid());
            boolean addToSaves = false;
            if (hasUpdatedClientData(schActivity, dbActivity)) {
                dbActivity.setClientData(schActivity.getClientData());
                addToSaves = true;
            }
            if (schActivity.getStartedOn() != null) {
                dbActivity.setStartedOn(schActivity.getStartedOn());
                addToSaves = true;
            }
            if (schActivity.getFinishedOn() != null) {
                dbActivity.setFinishedOn(schActivity.getFinishedOn());
                activityEventService.publishActivityFinishedEvent(dbActivity);
                addToSaves = true;
            }
            if (addToSaves) {
                activitiesToSave.add(dbActivity);
            }
        }
        activityDao.updateActivities(healthCode, activitiesToSave);
    }

    public void deleteActivitiesForUser(String healthCode) {
        checkArgument(isNotBlank(healthCode));

        activityDao.deleteActivitiesForUser(healthCode);
    }

    protected List<ScheduledActivity> orderActivities(List<ScheduledActivity> activities,
            Predicate<ScheduledActivity> filter) {
        return activities.stream()
            .filter(filter)
            .sorted(comparing(ScheduledActivity::getScheduledOn))
            .collect(toImmutableList());
    }

    /**
     * If the client data is being added or removed, or if it is different, then the activity is being
     * updated.
     */
    protected boolean hasUpdatedClientData(ScheduledActivity schActivity, ScheduledActivity dbActivity) {
        JsonNode schNode = (schActivity == null) ? null : schActivity.getClientData();
        JsonNode dbNode = (dbActivity == null) ? null : dbActivity.getClientData();
        return !Objects.equals(schNode, dbNode);
    }

    private int byteLength(JsonNode node) {
        try {
            return (node == null) ? 0 : node.toString().getBytes("UTF-8").length;
        } catch(UnsupportedEncodingException e) {
            return Integer.MAX_VALUE; // UTF-8 is always supported, this should *never* happen
        }
    }

    private Map<String, DateTime> createEventsMap(ScheduleContext context) {
        Map<String,DateTime> events = activityEventService.getActivityEventMap(context.getCriteriaContext().getHealthCode());

        ImmutableMap.Builder<String,DateTime> builder = new ImmutableMap.Builder<String, DateTime>();
        if (!events.containsKey(ENROLLMENT)) {
            builder.put(ENROLLMENT, context.getAccountCreatedOn().withZone(context.getInitialTimeZone()));
        }
        for(Map.Entry<String, DateTime> entry : events.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().withZone(context.getInitialTimeZone()));
        }
        return builder.build();
    }

    protected List<ScheduledActivity> scheduleActivitiesForPlans(ScheduleContext context) {
        // Cache compound activity defs, schemas, and surveys to reduce calls from duplicate requests.
        Map<String, CompoundActivity> compoundActivityCache = new HashMap<>();
        Map<String, SchemaReference> schemaCache = new HashMap<>();
        Map<String, SurveyReference> surveyCache = new HashMap<>();
        List<ScheduledActivity> scheduledActivities = new ArrayList<>();

        List<SchedulePlan> plans = schedulePlanService.getSchedulePlans(context.getCriteriaContext().getClientInfo(),
                context.getCriteriaContext().getStudyIdentifier());

        for (SchedulePlan plan : plans) {
            Schedule schedule = plan.getStrategy().getScheduleForUser(plan, context);
            if (schedule != null) {
                List<ScheduledActivity> activities = schedule.getScheduler().getScheduledActivities(plan, context);
                List<ScheduledActivity> resolvedActivities = resolveLinks(context.getCriteriaContext(),
                        compoundActivityCache, schemaCache, surveyCache, activities);
                scheduledActivities.addAll(resolvedActivities);
            }
        }
        return scheduledActivities;
    }

    private List<ScheduledActivity> resolveLinks(CriteriaContext context,
            Map<String, CompoundActivity> compoundActivityCache, Map<String, SchemaReference> schemaCache,
            Map<String, SurveyReference> surveyCache, List<ScheduledActivity> activities) {

        return activities.stream().map(schActivity -> {
            Activity activity = schActivity.getActivity();
            ActivityType activityType = activity.getActivityType();

            // Multiplex on activity type and resolve the activity, as needed.
            Activity resolvedActivity = null;
            if (activityType == ActivityType.COMPOUND) {
                // Resolve compound activity.
                CompoundActivity compoundActivity = activity.getCompoundActivity();
                CompoundActivity resolvedCompoundActivity = resolveCompoundActivity(context, compoundActivityCache,
                        schemaCache, surveyCache, compoundActivity);

                // If resolution changed the compound activity, generate a new activity instance that contains it.
                if (!resolvedCompoundActivity.equals(compoundActivity)) {
                    resolvedActivity = new Activity.Builder().withActivity(activity)
                            .withCompoundActivity(resolvedCompoundActivity).build();
                }
            } else if (activityType == ActivityType.SURVEY) {
                SurveyReference surveyRef = activity.getSurvey();
                SurveyReference resolvedSurveyRef = resolveSurvey(context, surveyCache, surveyRef);

                if (!resolvedSurveyRef.equals(surveyRef)) {
                    resolvedActivity = new Activity.Builder().withActivity(activity).withSurvey(resolvedSurveyRef)
                            .build();
                }
            } else if (activityType == ActivityType.TASK) {
                TaskReference taskRef = activity.getTask();
                SchemaReference schemaRef = taskRef.getSchema();

                if (schemaRef != null) {
                    SchemaReference resolvedSchemaRef = resolveSchema(context, schemaCache, schemaRef);

                    if (!resolvedSchemaRef.equals(schemaRef)) {
                        TaskReference resolvedTaskRef = new TaskReference(taskRef.getIdentifier(), resolvedSchemaRef);
                        resolvedActivity = new Activity.Builder().withActivity(activity).withTask(resolvedTaskRef)
                                .build();
                    }
                }
            }

            // Set the activity back into the ScheduledActivity, if needed.
            if (resolvedActivity != null) {
                schActivity.setActivity(resolvedActivity);
            }
            return schActivity;
        }).collect(toList());
    }

    // Helper method to resolve a compound activity reference from its definition.
    private CompoundActivity resolveCompoundActivity(CriteriaContext context,
            Map<String, CompoundActivity> compoundActivityCache, Map<String, SchemaReference> schemaCache,
            Map<String, SurveyReference> surveyCache, CompoundActivity compoundActivity) {
        String taskId = compoundActivity.getTaskIdentifier();
        CompoundActivity resolvedCompoundActivity = compoundActivityCache.get(taskId);
        if (resolvedCompoundActivity == null) {
            if (compoundActivity.isReference()) {
                // Compound activity has no schemas or surveys defined. Resolve it with its definition.
                CompoundActivityDefinition compoundActivityDef = compoundActivityDefinitionService
                        .getCompoundActivityDefinition(context.getStudyIdentifier(), taskId);
                resolvedCompoundActivity = compoundActivityDef.getCompoundActivity();
            } else {
                // Compound activity has schemas and surveys defined. Use the schemas and surveys from the lists, but
                // we may need to resolve individual schema and survey refs at a later step.
                resolvedCompoundActivity = compoundActivity;
            }

            // Before we cache it, resolve the surveys and schemas in the list.
            resolvedCompoundActivity = resolveListsInCompoundActivity(context, schemaCache, surveyCache,
                    resolvedCompoundActivity);

            compoundActivityCache.put(taskId, resolvedCompoundActivity);
        }
        return resolvedCompoundActivity;
    }

    // Helper method to resolve schema refs and survey refs inside of a compound activity.
    private CompoundActivity resolveListsInCompoundActivity(CriteriaContext context,
            Map<String, SchemaReference> schemaCache, Map<String, SurveyReference> surveyCache,
            CompoundActivity compoundActivity) {
        // Resolve schemas.
        // Lists in CompoundActivity are always non-null, so we don't need to null-check.
        boolean isModified = false;
        List<SchemaReference> schemaList = new ArrayList<>();
        for (SchemaReference oneSchemaRef : compoundActivity.getSchemaList()) {
            SchemaReference resolvedSchemaRef = resolveSchema(context, schemaCache, oneSchemaRef);
            schemaList.add(resolvedSchemaRef);

            if (!resolvedSchemaRef.equals(oneSchemaRef)) {
                // Only mark the compound activity as dirty if resolution actually changed the schema.
                isModified = true;
            }
        }

        // Similarly, resolve surveys.
        List<SurveyReference> surveyList = new ArrayList<>();
        for (SurveyReference oneSurveyRef : compoundActivity.getSurveyList()) {
            SurveyReference resolvedSurveyRef = resolveSurvey(context, surveyCache, oneSurveyRef);
            surveyList.add(resolvedSurveyRef);

            if (!resolvedSurveyRef.equals(oneSurveyRef)) {
                isModified = true;
            }
        }

        if (!isModified) {
            // Resolution didn't change any of our schema and survey refs. Just return the compound activity as is.
            return compoundActivity;
        } else {
            // We need to make a new compound activity with the resolved schemas and surveys.
            return new CompoundActivity.Builder().copyOf(compoundActivity).withSchemaList(schemaList)
                    .withSurveyList(surveyList).build();
        }
    }

    // Helper method to resolve a schema ref to the latest revision for the client.
    private SchemaReference resolveSchema(CriteriaContext context, Map<String, SchemaReference> schemaCache,
            SchemaReference schemaRef) {
        if (schemaRef.getRevision() != null) {
            // Already has a revision. No need to resolve. Return as is.
            return schemaRef;
        }

        String schemaId = schemaRef.getId();
        SchemaReference resolvedSchemaRef = schemaCache.get(schemaId);
        if (resolvedSchemaRef == null) {
            UploadSchema schema = schemaService.getLatestUploadSchemaRevisionForAppVersion(
                    context.getStudyIdentifier(), schemaId, context.getClientInfo());
            resolvedSchemaRef = new SchemaReference(schemaId, schema.getRevision());
            schemaCache.put(schemaId, resolvedSchemaRef);
        }
        return resolvedSchemaRef;
    }

    // Helper method to resolve a published survey to a specific survey version.
    private SurveyReference resolveSurvey(CriteriaContext context, Map<String, SurveyReference> surveyCache,
            SurveyReference surveyRef) {
        if (surveyRef.getCreatedOn() != null) {
            return surveyRef;
        }

        String surveyGuid = surveyRef.getGuid();
        SurveyReference resolvedSurveyRef = surveyCache.get(surveyGuid);
        if (resolvedSurveyRef == null) {
            Survey survey = surveyService.getSurveyMostRecentlyPublishedVersion(context.getStudyIdentifier(),
                    surveyGuid);
            resolvedSurveyRef = new SurveyReference(survey.getIdentifier(), surveyGuid,
                    new DateTime(survey.getCreatedOn()));
            surveyCache.put(surveyGuid, resolvedSurveyRef);
        }
        return resolvedSurveyRef;
    }
}
