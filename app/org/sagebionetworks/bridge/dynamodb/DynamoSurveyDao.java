package org.sagebionetworks.bridge.dynamodb;

import java.util.List;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.sagebionetworks.bridge.dao.ConcurrentModificationException;
import org.sagebionetworks.bridge.dao.PublishedSurveyException;
import org.sagebionetworks.bridge.dao.SurveyDao;
import org.sagebionetworks.bridge.dao.SurveyNotFoundException;
import org.sagebionetworks.bridge.exceptions.BridgeServiceException;
import org.sagebionetworks.bridge.models.surveys.Survey;
import org.sagebionetworks.bridge.models.surveys.SurveyQuestion;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.ConsistentReads;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig.SaveBehavior;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.QueryResultPage;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.google.common.collect.Lists;

public class DynamoSurveyDao implements SurveyDao {
    
    class QueryBuilder {
        
        DynamoDBQueryExpression<DynamoSurvey> query;
        String surveyGuid;
        String studyKey;
        long versionedOn;
        boolean published;
        
        QueryBuilder() {
            query = new DynamoDBQueryExpression<DynamoSurvey>();
            query.withScanIndexForward(false);
        }
        
        QueryBuilder setSurvey(String surveyGuid) {
            this.surveyGuid = surveyGuid;
            return this;
        }
        
        QueryBuilder setStudy(String studyKey) {
            this.studyKey = studyKey;
            return this;
        }
        
        QueryBuilder setVersionedOn(long versionedOn) {
            this.versionedOn = versionedOn;
            return this;
        }
        
        QueryBuilder isPublished() {
            this.published = true;
            return this;
        }
        
        List<Survey> get() {
            List<DynamoSurvey> dynamoSurveys = (surveyGuid == null) ? scan() : query();

            if (dynamoSurveys.size() == 0) {
                throw new SurveyNotFoundException(studyKey, surveyGuid, versionedOn);
            }
            List<Survey> surveys = Lists.newArrayListWithCapacity(dynamoSurveys.size());
            for (DynamoSurvey s : dynamoSurveys) {
                surveys.add((Survey)s);
            }
            return surveys;
        }
        
        Survey getOne() {
            List<Survey> surveys = get();
            attachQuestions(surveys.get(0));
            return surveys.get(0);
        }

        private List<DynamoSurvey> query() {
            query.withHashKeyValues(new DynamoSurvey(surveyGuid));    
            if (studyKey != null) {
                query.withQueryFilterEntry("studyKey", studyCondition());
            }
            if (versionedOn != 0L) {
                query.withRangeKeyCondition("versionedOn", versionedOnCondition());
            }
            if (published) {
                query.withQueryFilterEntry("published", publishedCondition());
            }
            return surveyMapper.queryPage(DynamoSurvey.class, query).getResults();
        }

        private List<DynamoSurvey> scan() {
            DynamoDBScanExpression scan = new DynamoDBScanExpression();
            if (studyKey != null) {
                scan.addFilterCondition("studyKey", studyCondition());
            }
            if (versionedOn != 0L) {
                scan.addFilterCondition("versionedOn", versionedOnCondition());
            }
            if (published) {
                scan.addFilterCondition("published", publishedCondition());
            }
            return surveyMapper.scan(DynamoSurvey.class, scan);
        }

        private Condition publishedCondition() {
            Condition condition = new Condition();
            condition.withComparisonOperator(ComparisonOperator.EQ);
            condition.withAttributeValueList(new AttributeValue().withN("1"));
            return condition;
        }

        private Condition studyCondition() {
            Condition studyCond = new Condition();
            studyCond.withComparisonOperator(ComparisonOperator.EQ);
            studyCond.withAttributeValueList(new AttributeValue().withS(studyKey));
            return studyCond;
        }

        private Condition versionedOnCondition() {
            Condition rangeCond = new Condition();
            rangeCond.withComparisonOperator(ComparisonOperator.EQ);
            rangeCond.withAttributeValueList(new AttributeValue().withN(Long.toString(versionedOn)));
            return rangeCond;
        }
        
        private void attachQuestions(Survey survey) {
            DynamoSurveyQuestion template = new DynamoSurveyQuestion();
            template.setSurveyGuid(survey.getGuid());
            
            DynamoDBQueryExpression<DynamoSurveyQuestion> query = new DynamoDBQueryExpression<DynamoSurveyQuestion>();
            query.withHashKeyValues(template);
            
            QueryResultPage<DynamoSurveyQuestion> page = surveyQuestionMapper.queryPage(DynamoSurveyQuestion.class,
                    query);
            
            List<SurveyQuestion> questions = Lists.newArrayList();
            for (DynamoSurveyQuestion question : page.getResults()) {
                questions.add((SurveyQuestion)question);
            }
            survey.setQuestions(questions);
        }
    }

    private DynamoDBMapper surveyMapper;
    private DynamoDBMapper surveyQuestionMapper;

    public void setDynamoDbClient(AmazonDynamoDB client) {
        DynamoDBMapperConfig mapperConfig = new DynamoDBMapperConfig(
                SaveBehavior.UPDATE,
                ConsistentReads.CONSISTENT,
                TableNameOverrideFactory.getTableNameOverride(DynamoSurvey.class));
        surveyMapper = new DynamoDBMapper(client, mapperConfig);
        
        mapperConfig = new DynamoDBMapperConfig(
                SaveBehavior.UPDATE,
                ConsistentReads.CONSISTENT,
                TableNameOverrideFactory.getTableNameOverride(DynamoSurveyQuestion.class));
        surveyQuestionMapper = new DynamoDBMapper(client, mapperConfig);
    }
    
    @Override
    public Survey createSurvey(Survey survey) {
        if (!isNew(survey)) {
            throw new BridgeServiceException("Cannot create an already created survey", 400);
        } else if (!isValid(survey)) {
            throw new BridgeServiceException("Survey is invalid (most likely missing required fields)", 400);
        }
        survey.setGuid(generateId());
        survey.setVersionedOn(DateTime.now(DateTimeZone.UTC).getMillis());
        
        return saveSurvey(survey);
    }

    @Override
    public Survey publishSurvey(Survey surveyKey) {
        Survey survey = getSurvey(surveyKey.getStudyKey(), surveyKey.getGuid(), surveyKey.getVersionedOn());
        if (survey.isPublished()) {
            return survey;
        }
        try {
            Survey existingPublished = getPublishedSurvey(survey.getStudyKey(), survey.getGuid());
            existingPublished.setPublished(false);
            surveyMapper.save(existingPublished);
        } catch(SurveyNotFoundException snfe) {
            // This is fine
        } catch(ConditionalCheckFailedException e) {
            throw new ConcurrentModificationException();
        }
        survey.setPublished(true);
        try {
            surveyMapper.save(survey);
        } catch(ConditionalCheckFailedException e) {
            throw new ConcurrentModificationException();
        }
        return survey;
    }
    
    @Override
    public Survey updateSurvey(Survey survey) {
        if (isNew(survey)) {
            throw new BridgeServiceException("Cannot update a new survey before you create it", 400);
        } else if (!isValid(survey)) {
            throw new BridgeServiceException("Survey is invalid (most likely missing required fields)", 400);
        }
        Survey existingSurvey = getSurvey(survey.getStudyKey(), survey.getGuid(), survey.getVersionedOn());
        if (existingSurvey.isPublished()) {
            throw new PublishedSurveyException(survey);
        }
        // Cannot change publication state from false on an update
        survey.setPublished(false);
        
        return saveSurvey(survey);
    }
    
    @Override
    public Survey versionSurvey(Survey surveyKey) {
        Survey existing = getSurvey(surveyKey.getStudyKey(), surveyKey.getGuid(), surveyKey.getVersionedOn());
        Survey copy = new DynamoSurvey(existing);
        copy.setPublished(false);
        copy.setVersion(null);
        copy.setVersionedOn(DateTime.now(DateTimeZone.UTC).getMillis());

        for (SurveyQuestion question : copy.getQuestions()) {
            question.setGuid(null);
        }
        return saveSurvey(copy);
    }

    @Override
    public List<Survey> getSurveys(String studyKey) {
        if (StringUtils.isBlank(studyKey)) {
            throw new BridgeServiceException("Study key is required", 400);
        }
        return new QueryBuilder().setStudy(studyKey).get();
    }
    
    @Override
    public List<Survey> getSurveys(String studyKey, String surveyGuid) {
        if (StringUtils.isBlank(studyKey)) {
            throw new BridgeServiceException("Study key is required", 400);
        } else if (StringUtils.isBlank(surveyGuid)) {
            throw new BridgeServiceException("Survey GUID is required", 400);
        }
        return new QueryBuilder().setStudy(studyKey).setSurvey(surveyGuid).get();
    }

    @Override
    public Survey getSurvey(String studyKey, String surveyGuid, long versionedOn) {
        if (StringUtils.isBlank(studyKey)) {
            throw new BridgeServiceException("Survey study key cannot be null/blank", 400);
        } else if (StringUtils.isBlank(surveyGuid)) {
            throw new BridgeServiceException("Survey GUID cannot be null/blank", 400);
        } else if (versionedOn == 0L) {
            throw new BridgeServiceException("Survey must have versionedOn date", 400);
        }
        return new QueryBuilder().setStudy(studyKey).setSurvey(surveyGuid).setVersionedOn(versionedOn).getOne();
    }
    
    @Override
    public Survey getPublishedSurvey(String studyKey, String surveyGuid) {
        List<Survey> surveys = new QueryBuilder().setStudy(studyKey).setSurvey(surveyGuid).isPublished().get();
        if (surveys.size() > 1) {
            throw new BridgeServiceException("Invalid state, survey has multiple versions ("+surveyGuid+")", 500);
        }            
        return surveys.get(0);
    }

    @Override
    public void deleteSurvey(Survey surveyKey) {
        Survey existing = getSurvey(surveyKey.getStudyKey(), surveyKey.getGuid(), surveyKey.getVersionedOn());
        if (existing.isPublished()) {
            // something has to happen here, this could be bad. Might need to publish another survey?
        }
        surveyMapper.batchDelete(existing.getQuestions());
        surveyMapper.delete(existing);
    }
    
    @Override
    public Survey closeSurvey(Survey surveyKey) {
        Survey existing = getSurvey(surveyKey.getStudyKey(), surveyKey.getGuid(), surveyKey.getVersionedOn());
        if (!existing.isPublished()) {
            throw new PublishedSurveyException(existing); 
        }
        existing.setPublished(false);
        try {
            surveyMapper.save(existing);
        } catch(ConditionalCheckFailedException e) {
            throw new ConcurrentModificationException();
        }
        return existing;
        // TODO:
        // Eventually this must do much more than this, it must also close out any existing 
        // survey response records.
    }
    
    private String generateId() {
        return UUID.randomUUID().toString();
    }
    
    private Survey saveSurvey(Survey survey) {
        deleteAllQuestions(survey.getGuid());
        List<SurveyQuestion> questions = survey.getQuestions();
        for (int i=0; i < questions.size(); i++) {
            SurveyQuestion question = questions.get(i);
            question.setSurveyGuid(survey.getGuid());
            question.setOrder(i);
            // A new question was added to the survey.
            if (question.getGuid() == null) {
                question.setGuid(generateId());
            }
            try {
                surveyQuestionMapper.save(question);    
            } catch(ConditionalCheckFailedException e) {
                throw new ConcurrentModificationException();
            }
        }
        try {
            surveyMapper.save(survey);    
        } catch(ConditionalCheckFailedException e) {
            throw new ConcurrentModificationException();
        }
        return survey;
    }
    
    private void deleteAllQuestions(String surveyGuid) {
        DynamoSurveyQuestion template = new DynamoSurveyQuestion();
        template.setSurveyGuid(surveyGuid);
        
        DynamoDBQueryExpression<DynamoSurveyQuestion> query = new DynamoDBQueryExpression<DynamoSurveyQuestion>();
        query.withHashKeyValues(template);
        
        QueryResultPage<DynamoSurveyQuestion> page = surveyQuestionMapper.queryPage(DynamoSurveyQuestion.class, query);

        for (DynamoSurveyQuestion question :  page.getResults()) {
            surveyQuestionMapper.delete(question);
        }
    }

    private boolean isNew(Survey survey) {
        if (survey.getGuid() != null || survey.isPublished() || survey.getVersion() != null || survey.getVersionedOn() != 0L) {
            return false;
        }
        for (SurveyQuestion question : survey.getQuestions()) {
            if (question.getGuid() != null) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isValid(Survey survey) {
        if (StringUtils.isBlank(survey.getIdentifier()) || StringUtils.isBlank(survey.getStudyKey())) {
            return false;
        }
        for (SurveyQuestion question : survey.getQuestions()) {
            if (StringUtils.isBlank(question.getIdentifier())) {
                return false;
            }
        }
        return true;
    }

}
