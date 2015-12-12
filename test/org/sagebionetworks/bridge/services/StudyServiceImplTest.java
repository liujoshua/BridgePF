package org.sagebionetworks.bridge.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import javax.annotation.Resource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.sagebionetworks.bridge.TestUserAdminHelper;
import org.sagebionetworks.bridge.TestUserAdminHelper.TestUser;
import org.sagebionetworks.bridge.TestUtils;
import org.sagebionetworks.bridge.cache.CacheProvider;
import org.sagebionetworks.bridge.dao.DirectoryDao;
import org.sagebionetworks.bridge.dao.StudyConsentDao;
import org.sagebionetworks.bridge.dao.SubpopulationDao;
import org.sagebionetworks.bridge.dao.ParticipantOption.SharingScope;
import org.sagebionetworks.bridge.dynamodb.DynamoStudy;
import org.sagebionetworks.bridge.exceptions.EntityAlreadyExistsException;
import org.sagebionetworks.bridge.exceptions.EntityNotFoundException;
import org.sagebionetworks.bridge.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.exceptions.UnauthorizedException;
import org.sagebionetworks.bridge.models.accounts.ConsentStatus;
import org.sagebionetworks.bridge.models.accounts.User;
import org.sagebionetworks.bridge.models.studies.EmailTemplate;
import org.sagebionetworks.bridge.models.studies.MimeType;
import org.sagebionetworks.bridge.models.studies.PasswordPolicy;
import org.sagebionetworks.bridge.models.studies.Study;
import org.sagebionetworks.bridge.models.subpopulations.ConsentSignature;
import org.sagebionetworks.bridge.models.subpopulations.StudyConsentView;
import org.sagebionetworks.bridge.models.subpopulations.Subpopulation;
import org.sagebionetworks.bridge.models.subpopulations.SubpopulationGuid;
import org.sagebionetworks.bridge.redis.JedisOps;
import org.sagebionetworks.bridge.redis.RedisKey;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@ContextConfiguration("classpath:test-context.xml")
@RunWith(SpringJUnit4ClassRunner.class)
public class StudyServiceImplTest {

    private static final String NUM_PARTICIPANTS_KEY = RedisKey.NUM_OF_PARTICIPANTS.getRedisKey("test");
    
    @Resource
    private JedisOps jedisOps;

    @Resource
    StudyServiceImpl studyService;
    
    @Resource
    StudyConsentServiceImpl studyConsentService;
    
    @Resource
    StudyConsentDao studyConsentDao;
    
    @Resource
    DirectoryDao directoryDao;
    
    @Resource
    SubpopulationDao subpopDao;
    
    @Resource
    SubpopulationService subpopService;
    
    @Resource
    ConsentService userConsentService;
    
    @Resource
    TestUserAdminHelper helper;
    
    private CacheProvider cache;
    
    private Study study;
    
    @Before
    public void before() {
        cache = mock(CacheProvider.class);
        studyService.setCacheProvider(cache);
    }
    
    @After
    public void after() {
        if (study != null) {
            studyService.deleteStudy(study.getIdentifier());
        }
    }
    
    @Test(expected=InvalidEntityException.class)
    public void studyIsValidated() {
        Study testStudy = new DynamoStudy();
        testStudy.setName("Belgian Waffles [Test]");
        studyService.createStudy(testStudy);
    }
    
    @Test
    public void cannotCreateAnExistingStudyWithAVersion() {
        study = TestUtils.getValidStudy(StudyServiceImplTest.class);
        study = studyService.createStudy(study);
        try {
            study = studyService.createStudy(study);
            fail("Should have thrown an exception");
        } catch(EntityAlreadyExistsException e) {
        }
    }
    
    @Test(expected=EntityAlreadyExistsException.class)
    public void cannotCreateAStudyWithAVersion() {
        Study testStudy = TestUtils.getValidStudy(StudyServiceImplTest.class);
        testStudy.setVersion(1L);
        testStudy = studyService.createStudy(testStudy);
    }
    
    @Test
    public void crudStudy() {
        study = TestUtils.getValidStudy(StudyServiceImplTest.class);
        // verify this can be null, that's okay, and the flags are reset correctly on create
        study.setTaskIdentifiers(null);
        study.setActive(false);
        study.setStrictUploadValidationEnabled(false);
        study.setHealthCodeExportEnabled(true);
        study = studyService.createStudy(study);
        
        assertNotNull("Version has been set", study.getVersion());
        assertTrue(study.isActive());
        assertTrue(study.isStrictUploadValidationEnabled()); // by default set to true
        assertTrue(study.isHealthCodeExportEnabled()); // it was set true in the study

        verify(cache).setStudy(study);
        verifyNoMoreInteractions(cache);
        reset(cache);
        
        // A default, active consent should be created for the study.
        StudyConsentView view = studyConsentService.getActiveConsent(SubpopulationGuid.create(study.getIdentifier()));
        assertTrue(view.getDocumentContent().contains("This is a placeholder for your consent document."));
        assertTrue(view.getActive());
        
        Study newStudy = studyService.getStudy(study.getIdentifier());
        assertTrue(newStudy.isActive());
        assertTrue(newStudy.isStrictUploadValidationEnabled());
        assertEquals(study.getIdentifier(), newStudy.getIdentifier());
        assertEquals("Test Study [StudyServiceImplTest]", newStudy.getName());
        assertEquals(200, newStudy.getMaxNumOfParticipants());
        assertEquals(18, newStudy.getMinAgeOfConsent());
        assertEquals(Sets.newHashSet("beta_users", "production_users"), newStudy.getDataGroups());
        assertEquals(0, newStudy.getTaskIdentifiers().size());
        // these should have been changed
        assertNotEquals("http://local-test-junk", newStudy.getStormpathHref());
        verify(cache).getStudy(newStudy.getIdentifier());
        verify(cache).setStudy(newStudy);
        verifyNoMoreInteractions(cache);
        reset(cache);

        studyService.deleteStudy(study.getIdentifier());
        verify(cache).getStudy(study.getIdentifier());
        verify(cache).setStudy(study);
        verify(cache).removeStudy(study.getIdentifier());
        try {
            studyService.getStudy(study.getIdentifier());
            fail("Should have thrown an exception");
        } catch(EntityNotFoundException e) {
        }
        // Verify that all the dependent stuff has been deleted as well:
        assertNull(directoryDao.getDirectoryForStudy(study));
        assertEquals(0, subpopDao.getSubpopulations(study.getStudyIdentifier(), false, true).size());
        assertEquals(0, studyConsentDao.getConsents(SubpopulationGuid.create(study.getIdentifier())).size());
        study = null;
    }
    
    @Test
    public void canUpdatePasswordPolicyAndEmailTemplates() {
        study = TestUtils.getValidStudy(StudyServiceImplTest.class);
        study.setPasswordPolicy(null);
        study.setVerifyEmailTemplate(null);
        study.setResetPasswordTemplate(null);
        study = studyService.createStudy(study);

        // First, verify that defaults are set...
        PasswordPolicy policy = study.getPasswordPolicy();
        assertNotNull(policy);
        assertEquals(8, policy.getMinLength());
        assertTrue(policy.isNumericRequired());
        assertTrue(policy.isSymbolRequired());
        assertTrue(policy.isUpperCaseRequired());

        EmailTemplate veTemplate = study.getVerifyEmailTemplate();
        assertNotNull(veTemplate);
        assertNotNull(veTemplate.getSubject());
        assertNotNull(veTemplate.getBody());
        
        EmailTemplate rpTemplate = study.getResetPasswordTemplate();
        assertNotNull(rpTemplate);
        assertNotNull(rpTemplate.getSubject());
        assertNotNull(rpTemplate.getBody());
        
        // Now change them and verify they are changed.
        study.setPasswordPolicy(new PasswordPolicy(6, true, false, false, true));
        study.setVerifyEmailTemplate(new EmailTemplate("subject *", "body ${url} *", MimeType.TEXT));
        study.setResetPasswordTemplate(new EmailTemplate("subject **", "body ${url} **", MimeType.TEXT));
        
        study = studyService.updateStudy(study, true);
        policy = study.getPasswordPolicy();
        assertEquals(6, policy.getMinLength());
        assertTrue(policy.isNumericRequired());
        assertFalse(policy.isSymbolRequired());
        assertFalse(policy.isLowerCaseRequired());
        assertTrue(policy.isUpperCaseRequired());
        
        veTemplate = study.getVerifyEmailTemplate();
        assertEquals("subject *", veTemplate.getSubject());
        assertEquals("body ${url} *", veTemplate.getBody());
        assertEquals(MimeType.TEXT, veTemplate.getMimeType());
        
        rpTemplate = study.getResetPasswordTemplate();
        assertEquals("subject **", rpTemplate.getSubject());
        assertEquals("body ${url} **", rpTemplate.getBody());
        assertEquals(MimeType.TEXT, rpTemplate.getMimeType());
    }
    
    @Test
    public void defaultsAreUsedWhenNotProvided() {
        study = TestUtils.getValidStudy(StudyServiceImplTest.class);
        study.setPasswordPolicy(null);
        study.setVerifyEmailTemplate(null);
        study.setResetPasswordTemplate(new EmailTemplate("   ", null, MimeType.TEXT));
        study = studyService.createStudy(study);
        
        assertEquals(PasswordPolicy.DEFAULT_PASSWORD_POLICY, study.getPasswordPolicy());
        assertNotNull(study.getVerifyEmailTemplate());
        assertNotNull(study.getResetPasswordTemplate());
        assertNotNull(study.getResetPasswordTemplate().getSubject());
        assertNotNull(study.getResetPasswordTemplate().getBody());
    }
    
    @Test
    public void problematicHtmlIsRemovedFromTemplates() {
        study = TestUtils.getValidStudy(StudyServiceImplTest.class);
        study.setVerifyEmailTemplate(new EmailTemplate("<b>This is not allowed [ve]</b>", "<p>Test [ve] ${url}</p><script></script>", MimeType.HTML));
        study.setResetPasswordTemplate(new EmailTemplate("<b>This is not allowed [rp]</b>", "<p>Test [rp] ${url}</p>", MimeType.TEXT));
        study = studyService.createStudy(study);
        
        EmailTemplate template = study.getVerifyEmailTemplate();
        assertEquals("This is not allowed [ve]", template.getSubject());
        assertEquals("<p>Test [ve] ${url}</p>", template.getBody());
        assertEquals(MimeType.HTML, template.getMimeType());
        
        template = study.getResetPasswordTemplate();
        assertEquals("This is not allowed [rp]", template.getSubject());
        assertEquals("Test [rp] ${url}", template.getBody());
        assertEquals(MimeType.TEXT, template.getMimeType());
    }
    
    @Test
    public void adminsCanSomeValuesResearchersCannot() {
        study = TestUtils.getValidStudy(StudyServiceImplTest.class);
        study.setMaxNumOfParticipants(200);
        study.setHealthCodeExportEnabled(false);
        study = studyService.createStudy(study);
        
        // Okay, now that these are set, researchers cannot change them
        study.setMaxNumOfParticipants(1000);
        study.setHealthCodeExportEnabled(true);
        study = studyService.updateStudy(study, false); // nope
        assertEquals(200, study.getMaxNumOfParticipants());
        assertFalse("This should be null", study.isHealthCodeExportEnabled());
        
        // But administrators can
        study.setMaxNumOfParticipants(1000);
        study.setHealthCodeExportEnabled(true);
        study = studyService.updateStudy(study, true); // yep
        assertEquals(1000, study.getMaxNumOfParticipants());
        assertTrue(study.isHealthCodeExportEnabled());
    }
    
    @Test(expected=InvalidEntityException.class)
    public void updateWithNoTemplatesIsInvalid() {
        study = TestUtils.getValidStudy(StudyServiceImplTest.class);
        study = studyService.createStudy(study);
        
        study.setVerifyEmailTemplate(null);
        studyService.updateStudy(study, false);
    }

    @Test(expected = UnauthorizedException.class)
    public void cantDeleteApiStudy() {
        studyService.deleteStudy("api");
    }
    
    @Test
    public void enforcesStudyEnrollmentLimit() {
        User user = new User();
        user.setConsentStatuses(
            Lists.newArrayList(new ConsentStatus("Consent", "guid", true, true, true)));
        
        try {
            jedisOps.del(NUM_PARTICIPANTS_KEY);
            
            Study study = TestUtils.getValidStudy(ConsentServiceImplTest.class);
            study.setIdentifier("test");
            study.setMaxNumOfParticipants(2);

            // Set the cache so we avoid going to DynamoDB. We're testing the caching layer
            // in the service test, we'll test the DAO in the DAO test.
            jedisOps.del(NUM_PARTICIPANTS_KEY);

            boolean limit = studyService.isStudyAtEnrollmentLimit(study);
            assertFalse("No limit reached", limit);
            studyService.incrementStudyEnrollment(study, user);
            studyService.incrementStudyEnrollment(study, user);
            assertTrue("Limit reached", studyService.isStudyAtEnrollmentLimit(study));
            assertEquals("2", jedisOps.get(NUM_PARTICIPANTS_KEY));
        } finally {
            jedisOps.del(NUM_PARTICIPANTS_KEY);
        }
    }
    
    @Test
    public void studyEnrollmentNotIncrementedOnSubsequentConsents() {
        User user = new User();
        user.setConsentStatuses(
            Lists.newArrayList(new ConsentStatus("Consent", "guid", true, true, true)));
        
        try {
            jedisOps.del(NUM_PARTICIPANTS_KEY);
            jedisOps.setnx(NUM_PARTICIPANTS_KEY, "0");
            
            Study study = TestUtils.getValidStudy(ConsentServiceImplTest.class);
            study.setIdentifier("test");
            study.setMaxNumOfParticipants(2);

            studyService.incrementStudyEnrollment(study, user);
            assertFalse(studyService.isStudyAtEnrollmentLimit(study));
            
            // On subsequent consents, user is not added and enrollment is not increased
            user.setConsentStatuses(Lists.newArrayList(
                new ConsentStatus("Consent", "guid", true, true, true),
                new ConsentStatus("Consent", "guid2", true, true, true)
            ));
            studyService.incrementStudyEnrollment(study, user);
            studyService.incrementStudyEnrollment(study, user);
            studyService.incrementStudyEnrollment(study, user);
            assertFalse(studyService.isStudyAtEnrollmentLimit(study));
            assertEquals("1", jedisOps.get(NUM_PARTICIPANTS_KEY));
        } finally {
            jedisOps.del(NUM_PARTICIPANTS_KEY);
        }
    }
    
    @Test
    public void decrementingStudyWorks() {
        User user = new User();
        user.setConsentStatuses(
            Lists.newArrayList(new ConsentStatus("Consent", "guid", true, false, true)));
        
        try {
            jedisOps.del(NUM_PARTICIPANTS_KEY);
            jedisOps.setnx(NUM_PARTICIPANTS_KEY, "2");
            
            Study study = TestUtils.getValidStudy(ConsentServiceImplTest.class);
            study.setIdentifier("test");
            study.setMaxNumOfParticipants(2);

            studyService.decrementStudyEnrollment(study, user);
            assertFalse(studyService.isStudyAtEnrollmentLimit(study));
            
            studyService.decrementStudyEnrollment(study, user);
            studyService.decrementStudyEnrollment(study, user);
            studyService.decrementStudyEnrollment(study, user);
            studyService.decrementStudyEnrollment(study, user);
            assertFalse(studyService.isStudyAtEnrollmentLimit(study));
            assertEquals("0", jedisOps.get(NUM_PARTICIPANTS_KEY));
        } finally {
            jedisOps.del(NUM_PARTICIPANTS_KEY);
        }        
    }
    
    @Test
    public void studyEnrollmentNotDecrementedUntilLastWithdrawal() {
        User user = new User();
        user.setConsentStatuses(
            Lists.newArrayList(new ConsentStatus("Consent", "guid", true, true, true)));
        
        try {
            jedisOps.del(NUM_PARTICIPANTS_KEY);
            jedisOps.setnx(NUM_PARTICIPANTS_KEY, "2");
            
            Study study = TestUtils.getValidStudy(ConsentServiceImplTest.class);
            study.setIdentifier("test");
            study.setMaxNumOfParticipants(2);

            // With a consent, this does not decrement
            studyService.decrementStudyEnrollment(study, user);
            assertEquals("2", jedisOps.get(NUM_PARTICIPANTS_KEY));
            
            // With one consent not signed, this will decrement.
            user.setConsentStatuses(
                    Lists.newArrayList(new ConsentStatus("Consent", "guid", true, false, true)));
            studyService.decrementStudyEnrollment(study, user);
            assertEquals("1", jedisOps.get(NUM_PARTICIPANTS_KEY));
        } finally {
            jedisOps.del(NUM_PARTICIPANTS_KEY);
        }           
    }
    
    @Test
    public void getNumberOfParticipants() {
        Study study = TestUtils.getValidStudy(ConsentServiceImplTest.class);
        study = studyService.createStudy(study);
        
        Subpopulation subpop1 = Subpopulation.create();
        subpop1.setName("Group 1");
        subpopService.createSubpopulation(study, subpop1);
        
        Subpopulation subpop2 = Subpopulation.create();
        subpop2.setName("Group 2");
        subpopService.createSubpopulation(study, subpop2);
        
        TestUserAdminHelper.Builder builder = helper.getBuilder(StudyServiceImplTest.class).withStudy(study)
                .withConsent(true);
        
        TestUser user1 = builder.withSubpopulation(subpop1).build();
        TestUser user2 = builder.withSubpopulation(subpop1).build();
        TestUser user3 = builder.withSubpopulation(subpop1).build();
        // and user2 is also in the other subpop2, but will still be counted correctly.
        userConsentService.consentToResearch(study, subpop2, user2.getUser(), 
                new ConsentSignature.Builder().withBirthdate("1980-01-01").withName("Name").build(), SharingScope.NO_SHARING, false);
        try {

            long count = studyService.getNumberOfParticipants(study);
            assertEquals(3, count);
            
        } finally {
            helper.deleteUser(user1);
            helper.deleteUser(user2);
            helper.deleteUser(user3);
            studyService.deleteStudy(study.getIdentifier());
        }
    }
}
