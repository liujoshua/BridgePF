package org.sagebionetworks.bridge.services;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.UUID;

import org.sagebionetworks.bridge.dao.HealthCodeDao;
import org.sagebionetworks.bridge.dao.HealthIdDao;
import org.sagebionetworks.bridge.models.accounts.HealthId;
import org.sagebionetworks.bridge.models.accounts.HealthIdImpl;
import org.sagebionetworks.bridge.models.studies.StudyIdentifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HealthCodeService {

    private final Logger logger = LoggerFactory.getLogger(HealthCodeService.class);

    private HealthIdDao healthIdDao;
    private HealthCodeDao healthCodeDao;

    @Autowired
    public void setHealthIdDao(HealthIdDao healthIdDao) {
        this.healthIdDao = healthIdDao;
    }

    @Autowired
    public void setHealthCodeDao(HealthCodeDao healthCodeDao) {
        this.healthCodeDao = healthCodeDao;
    }

    public HealthId createMapping(StudyIdentifier studyIdentifier) {
        return createMapping(studyIdentifier, null);
    }

    /**
     * Returns a HealthId mapping by generating a new healthCode, and reusing the healthId if provided.
     *
     * @param studyIdentifier
     *         study identifier
     * @param healthId
     *         healthId, or null
     * @return
     */
    public HealthId createMapping(StudyIdentifier studyIdentifier, String healthId) {
        checkNotNull(studyIdentifier);

        final String healthCode = generateHealthCode(studyIdentifier.getIdentifier());
        healthId = generateHealthId(healthCode, healthId);

        return new HealthIdImpl(healthId, healthCode);
    }

    public HealthId getMapping(String healthId) {
        if (healthId == null) {
            return null;
        }
        final String healthCode = healthIdDao.getCode(healthId);
        if (healthCode == null) {
            return null;
        }
        return new HealthIdImpl(healthId, healthCode);
    }

    private String generateHealthCode(String studyId) {
        String code = UUID.randomUUID().toString();
        boolean isSet = healthCodeDao.setIfNotExist(code, studyId);
        while (!isSet) {
            logger.error("Health code " + code + " conflicts. This should never happen. " +
                    "Make sure the UUID generator is a solid one.");
            code = UUID.randomUUID().toString();
            isSet = healthCodeDao.setIfNotExist(code, studyId);
        }
        return code;
    }

    private String generateHealthId(final String healthCode, String healthId) {
        if (healthId == null) {
            healthId = UUID.randomUUID().toString();
        }

        boolean isSet = healthIdDao.setIfNotExist(healthId, healthCode);
        while (!isSet) {
            logger.error("Health ID " + healthId + " conflicts. This should never happen. " +
                    "Make sure the UUID generator is a solid one.");
            healthId = UUID.randomUUID().toString();
            isSet = healthIdDao.setIfNotExist(healthId, healthCode);
        }
        return healthId;
    }
}

