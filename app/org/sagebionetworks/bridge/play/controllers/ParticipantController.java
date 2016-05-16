package org.sagebionetworks.bridge.play.controllers;

import static java.lang.Integer.parseInt;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.sagebionetworks.bridge.Roles.RESEARCHER;
import static org.sagebionetworks.bridge.BridgeConstants.NO_CALLER_ROLES;

import java.util.Set;

import static org.sagebionetworks.bridge.BridgeConstants.API_DEFAULT_PAGE_SIZE;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import org.sagebionetworks.bridge.exceptions.BadRequestException;
import org.sagebionetworks.bridge.models.CriteriaContext;
import org.sagebionetworks.bridge.models.PagedResourceList;
import org.sagebionetworks.bridge.models.accounts.AccountSummary;
import org.sagebionetworks.bridge.models.accounts.IdentifierHolder;
import org.sagebionetworks.bridge.models.accounts.StudyParticipant;
import org.sagebionetworks.bridge.models.accounts.UserSession;
import org.sagebionetworks.bridge.models.accounts.UserSessionInfo;
import org.sagebionetworks.bridge.models.studies.Study;
import org.sagebionetworks.bridge.services.ParticipantService;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Sets;

import play.mvc.Result;

@Controller
public class ParticipantController extends BaseController {
    
    private ParticipantService participantService;
    
    @Autowired
    final void setParticipantService(ParticipantService participantService) {
        this.participantService = participantService;
    }
    
    public Result getSelfParticipant() {
        UserSession session = getAuthenticatedSession();
        Study study = studyService.getStudy(session.getStudyIdentifier());
        
        StudyParticipant participant = participantService.getParticipant(study, NO_CALLER_ROLES, session.getId());
        
        return okResult(participant);
    }
    
    public Result updateSelfParticipant() throws Exception {
        UserSession session = getAuthenticatedSession();
        Study study = studyService.getStudy(session.getStudyIdentifier());
        
        // By copying only values that were included in the JSON onto the existing StudyParticipant,
        // we allow clients to only send back partial JSON to update the user. This has been the 
        // usage pattern in prior APIs and it will make refactoring to use this API easier.
        JsonNode node = requestToJSON(request());
        Set<String> fieldNames = Sets.newHashSet(node.fieldNames());
        
        StudyParticipant participant = MAPPER.treeToValue(node, StudyParticipant.class);
        StudyParticipant existing = participantService.getParticipant(study, NO_CALLER_ROLES, session.getId());
        StudyParticipant updated = new StudyParticipant.Builder()
                .copyOf(existing)
                .copyFieldsOf(participant, fieldNames).build();
        
        participantService.updateParticipant(study, NO_CALLER_ROLES, session.getId(), updated);
        
        // Update this user's session (creates one if it doesn't exist, but this is safe)
        CriteriaContext context = getCriteriaContext(study.getStudyIdentifier());
        session = authenticationService.updateSession(study, context, session.getId());
        updateSession(session);
        
        return okResult(UserSessionInfo.toJSON(session));
    }
    
    public Result getParticipants(String offsetByString, String pageSizeString, String emailFilter) {
        UserSession session = getAuthenticatedSession(RESEARCHER);
        
        Study study = studyService.getStudy(session.getStudyIdentifier());
        int offsetBy = getIntOrDefault(offsetByString, 0);
        int pageSize = getIntOrDefault(pageSizeString, API_DEFAULT_PAGE_SIZE);
        
        PagedResourceList<AccountSummary> page = participantService.getPagedAccountSummaries(study, offsetBy, pageSize, emailFilter);
        return okResult(page);
    }
    
    public Result createParticipant() throws Exception {
        UserSession session = getAuthenticatedSession(RESEARCHER);
        Study study = studyService.getStudy(session.getStudyIdentifier());
        
        StudyParticipant participant = parseJson(request(), StudyParticipant.class);
        
        IdentifierHolder holder = participantService.createParticipant(study, session.getParticipant().getRoles(),
                participant, true);
        return createdResult(holder);
    }
    
    public Result getParticipant(String userId) {
        UserSession session = getAuthenticatedSession(RESEARCHER);
        Study study = studyService.getStudy(session.getStudyIdentifier());
        
        StudyParticipant participant = participantService.getParticipant(study,
                session.getParticipant().getRoles(), userId);
        return okResult(participant);
    }
    
    public Result updateParticipant(String userId) {
        UserSession session = getAuthenticatedSession(RESEARCHER);
        Study study = studyService.getStudy(session.getStudyIdentifier());

        StudyParticipant participant = parseJson(request(), StudyParticipant.class);
        // Just stop right here because something is wrong
        if (participant.getId() != null && !userId.equals(participant.getId())) {
            throw new BadRequestException("ID in JSON does not match email in URL.");
        }
        participantService.updateParticipant(study, session.getParticipant().getRoles(), userId, participant);
        
        // Push changes to the user's session, including consent statuses.
        CriteriaContext context = new CriteriaContext.Builder()
                .withStudyIdentifier(study.getStudyIdentifier()).build();
        session = authenticationService.updateSession(study, context, userId);
        updateSession(session);

        return okResult("Participant updated.");
    }
    
    public Result signOut(String userId) throws Exception {
        UserSession session = getAuthenticatedSession(RESEARCHER);
        Study study = studyService.getStudy(session.getStudyIdentifier());

        participantService.signUserOut(study, userId);

        return okResult("User signed out.");
    }

    private int getIntOrDefault(String value, int defaultValue) {
        if (isBlank(value)) {
            return defaultValue;
        }
        try {
            return parseInt(value);
        } catch(NumberFormatException e) {
            throw new BadRequestException(value + " is not an integer");
        }
    }

}
