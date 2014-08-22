package org.sagebionetworks.bridge.dao;

import org.sagebionetworks.bridge.models.ResearchConsent;
import org.sagebionetworks.bridge.models.StudyConsent;

public interface UserConsentDao {

    /**
     * Gives consent to the specified study.
     */
    void giveConsent(String healthCode, StudyConsent studyConsent, ResearchConsent researchConsent);

    /**
     * Withdraws consent to the specified study.
     */
    void withdrawConsent(String healthCode, StudyConsent studyConsent);

    /**
     * Gets the time stamp of the consent document the user has consented.
     * Returns null if the user has not consented yet.
     */
    Long getConsentCreatedOn(String healthCode, String studyKey);

    /**
     * Whether the user has consented to the specified study.
     */
    boolean hasConsented(String healthCode, StudyConsent studyConsent);

    /**
     * Resume sharing data for the study.
     */
    void resumeSharing(String healthCode, StudyConsent studyConsent);

    /**
     * Stop sharing data for the study.
     */
    void suspendSharing(String healthCode, StudyConsent studyConsent);

    /**
     * Whether the user has agreed to share data for the study.
     */
    boolean isSharingData(String healthCode, StudyConsent studyConsent);

    /**
     * Returns the consent signature, consisting of the signature name and birthdate.
     */
    ResearchConsent getConsentSignature(String healthCode, StudyConsent studyConsent);
}