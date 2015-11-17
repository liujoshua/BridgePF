package org.sagebionetworks.bridge.play.controllers;

import java.util.Set;

import org.sagebionetworks.bridge.BridgeConstants;
import org.sagebionetworks.bridge.cache.ViewCache;
import org.sagebionetworks.bridge.cache.ViewCache.ViewCacheKey;
import org.sagebionetworks.bridge.models.accounts.DataGroups;
import org.sagebionetworks.bridge.models.accounts.ExternalIdentifier;
import org.sagebionetworks.bridge.models.accounts.User;
import org.sagebionetworks.bridge.models.accounts.UserProfile;
import org.sagebionetworks.bridge.models.accounts.UserSession;
import org.sagebionetworks.bridge.models.studies.Study;
import org.sagebionetworks.bridge.services.ParticipantOptionsService;
import org.sagebionetworks.bridge.services.UserProfileService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import com.google.common.base.Supplier;

import play.mvc.Result;

@Controller
public class UserProfileController extends BaseController {
    
    private UserProfileService userProfileService;
    
    private ParticipantOptionsService optionsService;
    
    private ViewCache viewCache;

    @Autowired
    public void setUserProfileService(UserProfileService userProfileService) {
        this.userProfileService = userProfileService;
    }
    @Autowired
    public void setParticipantOptionsService(ParticipantOptionsService optionsService) {
        this.optionsService = optionsService;
    }
    @Autowired
    public void setViewCache(ViewCache viewCache) {
        this.viewCache = viewCache;
    }

    public Result getUserProfile() throws Exception {
        final UserSession session = getAuthenticatedSession();
        final Study study = studyService.getStudy(session.getStudyIdentifier());
        
        ViewCacheKey<UserProfile> cacheKey = viewCache.getCacheKey(UserProfile.class, session.getUser().getId(), study.getIdentifier());
        String json = viewCache.getView(cacheKey, new Supplier<UserProfile>() {
            @Override public UserProfile get() {
                return userProfileService.getProfile(study, session.getUser().getEmail());
            }
        });
        return ok(json).as(BridgeConstants.JSON_MIME_TYPE);
    }

    public Result updateUserProfile() throws Exception {
        UserSession session = getAuthenticatedSession();
        Study study = studyService.getStudy(session.getStudyIdentifier());
        
        User user = session.getUser();
        UserProfile profile = UserProfile.fromJson(study.getUserProfileAttributes(), requestToJSON(request()));
        user = userProfileService.updateProfile(study, user, profile);
        updateSessionUser(session, user);
        
        ViewCacheKey<UserProfile> cacheKey = viewCache.getCacheKey(UserProfile.class, session.getUser().getId(), study.getIdentifier());
        viewCache.removeView(cacheKey);
        
        return okResult("Profile updated.");
    }
    
    public Result createExternalIdentifier() throws Exception {
        UserSession session = getAuthenticatedSession();

        ExternalIdentifier externalId = parseJson(request(), ExternalIdentifier.class);
        optionsService.setExternalIdentifier(session.getStudyIdentifier(), session.getUser().getHealthCode(), externalId);
        return okResult("External identifier added to user profile.");
    }
    
    public Result getDataGroups() throws Exception {
        UserSession session = getAuthenticatedSession();

        Set<String> dataGroups = optionsService.getDataGroups(session.getUser().getHealthCode());
        return okResult(new DataGroups(dataGroups));
    }
    
    public Result updateDataGroups() throws Exception {
        UserSession session = getAuthenticatedSession();
        
        DataGroups dataGroups = parseJson(request(), DataGroups.class);
        
        optionsService.setDataGroups(session.getStudyIdentifier(), 
                session.getUser().getHealthCode(), dataGroups);
        return okResult("Data groups updated.");
    }

}
