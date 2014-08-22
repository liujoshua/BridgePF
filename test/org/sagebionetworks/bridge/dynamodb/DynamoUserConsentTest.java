package org.sagebionetworks.bridge.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class DynamoUserConsentTest {

    @Test
    public void test() {
        // Test constructor 1
        String healthCode = "123";
        String studyKey = "456";
        DynamoUserConsent2 userConsent = new DynamoUserConsent2(healthCode, studyKey);
        assertEquals(healthCode + ":" + studyKey, userConsent.getHealthCodeStudy());
        assertEquals(healthCode, userConsent.getHealthCode());
        assertEquals(studyKey, userConsent.getStudyKey());
        // Test constructor 2
        long consentTimestamp = 789L;
        DynamoStudyConsent1 studyConsent = new DynamoStudyConsent1();
        studyConsent.setStudyKey(studyKey);
        studyConsent.setCreatedOn(consentTimestamp);
        userConsent = new DynamoUserConsent2(healthCode, studyConsent);
        assertEquals(healthCode + ":" + studyKey, userConsent.getHealthCodeStudy());
        assertEquals(healthCode, userConsent.getHealthCode());
        assertEquals(studyKey, userConsent.getStudyKey());
        assertEquals(consentTimestamp, userConsent.getConsentCreatedOn());
        // Test copy constructor
        userConsent.setSignedOn(555L);
        userConsent.setDataSharing(true);
        userConsent.setName("name");
        userConsent.setBirthdate("birthdate");
        userConsent.setVersion(777L);
        DynamoUserConsent2 userConsentCopy = new DynamoUserConsent2(userConsent);
        assertEquals(healthCode + ":" + studyKey, userConsent.getHealthCodeStudy());
        assertEquals(healthCode, userConsent.getHealthCode());
        assertEquals(studyKey, userConsent.getStudyKey());
        assertEquals(consentTimestamp, userConsent.getConsentCreatedOn());
        assertEquals(555L, userConsent.getSignedOn());
        assertTrue(userConsent.getDataSharing());
        assertEquals("name", userConsent.getName());
        assertEquals("birthdate", userConsent.getBirthdate());
        assertEquals(userConsent.getVersion(), userConsentCopy.getVersion());
    }
}
