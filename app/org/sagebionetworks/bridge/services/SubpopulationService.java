package org.sagebionetworks.bridge.services;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.validation.Validator;

import org.sagebionetworks.bridge.BridgeUtils;
import org.sagebionetworks.bridge.dao.SubpopulationDao;
import org.sagebionetworks.bridge.models.schedules.ScheduleContext;
import org.sagebionetworks.bridge.models.studies.Study;
import org.sagebionetworks.bridge.models.studies.StudyIdentifier;
import org.sagebionetworks.bridge.models.subpopulations.StudyConsentForm;
import org.sagebionetworks.bridge.models.subpopulations.StudyConsentView;
import org.sagebionetworks.bridge.models.subpopulations.Subpopulation;
import org.sagebionetworks.bridge.models.subpopulations.SubpopulationGuid;
import org.sagebionetworks.bridge.validators.SubpopulationValidator;
import org.sagebionetworks.bridge.validators.Validate;

@Component
public class SubpopulationService {

    private SubpopulationDao subpopDao;
    private StudyConsentService studyConsentService;
    private StudyConsentForm defaultConsentDocument;
    
    @Autowired
    final void setSubpopulationDao(SubpopulationDao subpopDao) {
        this.subpopDao = subpopDao;
    }
    @Autowired
    final void setStudyConsentService(StudyConsentService studyConsentService) {
        this.studyConsentService = studyConsentService;
    }
    @Value("classpath:study-defaults/consent-body.xhtml")
    final void setDefaultConsentDocument(org.springframework.core.io.Resource resource) throws IOException {
        this.defaultConsentDocument = new StudyConsentForm(IOUtils.toString(resource.getInputStream(), StandardCharsets.UTF_8));
    }
    
    /**
     * Create subpopulation.
     * @param study 
     * @param subpop
     * @return
     */
    public Subpopulation createSubpopulation(Study study, Subpopulation subpop) {
        checkNotNull(study);
        checkNotNull(subpop);
        
        subpop.setGuid(BridgeUtils.generateGuid());
        subpop.setStudyIdentifier(study.getIdentifier());
        Validator validator = new SubpopulationValidator(study.getDataGroups());
        Validate.entityThrowingException(validator, subpop);
        
        // Create a default consent for this subpopulation.
        StudyConsentView view = studyConsentService.addConsent(subpop, defaultConsentDocument);
        studyConsentService.publishConsent(study, subpop, view.getCreatedOn());
        
        return subpopDao.createSubpopulation(subpop);
    }
    
    /**
     * Create a default subpopulation for a new study
     * @param study
     * @return
     */
    public Subpopulation createDefaultSubpopulation(Study study) {
        SubpopulationGuid subpopGuid = SubpopulationGuid.create(study.getIdentifier());
        // Migrating, studies will already have consents so don't create and publish a new one
        // unless this is part of the creation of a new study after the introduction of subpopulations.
        if (studyConsentService.getAllConsents(subpopGuid).isEmpty()) {
            StudyConsentView view = studyConsentService.addConsent(subpopGuid, defaultConsentDocument);
            studyConsentService.publishConsent(study, subpopGuid, view.getCreatedOn());
        }
        return subpopDao.createDefaultSubpopulation(study.getStudyIdentifier());
    }
    
    /**
     * Update a subpopulation.
     * @param study
     * @param subpop
     * @return
     */
    public Subpopulation updateSubpopulation(Study study, Subpopulation subpop) {
        checkNotNull(study);
        checkNotNull(subpop);
        
        subpop.setStudyIdentifier(study.getIdentifier());
        Validator validator = new SubpopulationValidator(study.getDataGroups());
        Validate.entityThrowingException(validator, subpop);
        
        return subpopDao.updateSubpopulation(subpop);
    }
    
    /**
     * Get all subpopulations defined for this study that have not been deleted. If 
     * there are no subpopulations, a default subpopulation will be created with a 
     * default consent.
     * @param studyId
     * @return
     */
    public List<Subpopulation> getSubpopulations(StudyIdentifier studyId) {
        checkNotNull(studyId);
        
        return subpopDao.getSubpopulations(studyId, true, false);
    }
    
    /**
     * Get a specific subpopulation.
     * @param studyId
     * @param subpopGuid
     * @return subpopulation
     */
    public Subpopulation getSubpopulation(StudyIdentifier studyId, SubpopulationGuid subpopGuid) {
        checkNotNull(studyId);
        checkNotNull(subpopGuid);
        
        return subpopDao.getSubpopulation(studyId, subpopGuid);
    }
    
    /**
     * Get a subpopulation that matches the most specific criteria defined for a subpopulation. 
     * That is, the populations are sorted by the amount of criteria that are defined to match that 
     * population, and the first one that matches is returned.
     * @param context
     * @return
     */
    public List<Subpopulation> getSubpopulationForUser(ScheduleContext context) {
        checkNotNull(context);
        
        return subpopDao.getSubpopulationsForUser(context);
    }

    /**
     * Delete a subpopulation.
     * @param studyId
     * @param subpopGuid
     */
    public void deleteSubpopulation(StudyIdentifier studyId, SubpopulationGuid subpopGuid) {
        checkNotNull(studyId);
        checkNotNull(subpopGuid);
        
        subpopDao.deleteSubpopulation(studyId, subpopGuid);
    }
    
    /**
     * Delete all the subpopulations for a study (being deleted).
     * @param studyId
     */
    public void deleteAllSubpopulations(StudyIdentifier studyId) {
        checkNotNull(studyId);
        
        subpopDao.deleteAllSubpopulations(studyId);
    }

}
