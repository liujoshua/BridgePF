package org.sagebionetworks.bridge.validators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.sagebionetworks.bridge.TestUtils;
import org.sagebionetworks.bridge.dynamodb.DynamoStudy;
import org.sagebionetworks.bridge.exceptions.InvalidEntityException;
import org.sagebionetworks.bridge.models.studies.EmailTemplate;
import org.sagebionetworks.bridge.models.studies.MimeType;
import org.sagebionetworks.bridge.models.studies.PasswordPolicy;
import org.sagebionetworks.bridge.models.studies.Study;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class StudyValidatorTest {

    private DynamoStudy study;
    
    @Before
    public void createValidStudy() {
        study = TestUtils.getValidStudy(StudyValidatorTest.class);
    }
    
    public void assertCorrectMessage(Study study, String fieldName, String message) {
        try {
            Validate.entityThrowingException(StudyValidator.INSTANCE, study);
            fail("should have thrown an exception");
        } catch(InvalidEntityException e) {
            List<String> errors = e.getErrors().get(fieldName);
            assertFalse(errors == null || errors.isEmpty());
            String error = errors.get(0);
            assertEquals(message, error);
        }
    }
    
    @Test
    public void acceptsValidStudy() {
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    // While 2 is not a good length, we must allow it for legacy reasons.
    @Test
    public void minLengthCannotBeLessThan2() {
        study.setPasswordPolicy(new PasswordPolicy(1, false, false, false, false));
        assertCorrectMessage(study, "passwordPolicy.minLength", "passwordPolicy.minLength must be 2-999 characters");
    }
    
    @Test
    public void sponsorNameRequired() {
        study.setSponsorName("");
        assertCorrectMessage(study, "sponsorName", "sponsorName is required");
    }
    
    @Test
    public void minLengthCannotBeMoreThan999() {
        study.setPasswordPolicy(new PasswordPolicy(1000, false, false, false, false));
        assertCorrectMessage(study, "passwordPolicy.minLength", "passwordPolicy.minLength must be 2-999 characters");
    }
    
    @Test
    public void resetPasswordMustHaveUrlVariable() {
        study.setResetPasswordTemplate(new EmailTemplate("subject", "no url variable", MimeType.TEXT));
        assertCorrectMessage(study, "resetPasswordTemplate.body", "resetPasswordTemplate.body must contain the ${url} template variable");
    }
    
    @Test
    public void verifyEmailMustHaveUrlVariable() {
        study.setVerifyEmailTemplate(new EmailTemplate("subject", "no url variable", MimeType.TEXT));
        assertCorrectMessage(study, "verifyEmailTemplate.body", "verifyEmailTemplate.body must contain the ${url} template variable");
    }

    @Test
    public void cannotCreateIdentifierWithUppercase() {
        study.setIdentifier("Test");
        assertCorrectMessage(study, "identifier", "identifier must contain only lower-case letters and/or numbers with optional dashes");
    }

    @Test
    public void cannotCreateInvalidIdentifierWithSpaces() {
        study.setIdentifier("test test");
        assertCorrectMessage(study, "identifier", "identifier must contain only lower-case letters and/or numbers with optional dashes");
    }

    @Test
    public void identifierCanContainDashes() {
        study.setIdentifier("sage-pd");
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    @Test
    public void acceptsMultipleValidSupportEmailAddresses() {
        study.setSupportEmail("test@test.com,test2@test.com");
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    @Test
    public void rejectsInvalidSupportEmailAddresses() {
        study.setSupportEmail("test@test.com,asdf,test2@test.com");
        assertCorrectMessage(study, "supportEmail", "supportEmail 'asdf' is not a valid email address");
    }
    
    @Test
    public void requiresMissingSupportEmail() {
        study.setSupportEmail(null);
        assertCorrectMessage(study, "supportEmail", "supportEmail is required");
    }
    
    @Test
    public void acceptsMultipleValidTechnicalEmailAddresses() {
        study.setTechnicalEmail("test@test.com,test2@test.com");
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    @Test
    public void rejectsInvalidTechnicalEmailAddresses() {
        study.setTechnicalEmail("test@test.com,asdf,test2@test.com");
        assertCorrectMessage(study, "technicalEmail", "technicalEmail 'asdf' is not a valid email address");
    }
    
    @Test
    public void requiresMissingTechnicalEmail() {
        study.setTechnicalEmail(null);
        assertCorrectMessage(study, "technicalEmail", "technicalEmail is required");
    }
    
    @Test
    public void rejectsInvalidConsentEmailAddresses() {
        study.setConsentNotificationEmail("test@test.com,asdf,test2@test.com");
        assertCorrectMessage(study, "consentNotificationEmail", "consentNotificationEmail 'asdf' is not a valid email address");
    }
    
    @Test
    public void cannotAddConflictingEmailAttribute() {
        study.getUserProfileAttributes().add("email");
        assertCorrectMessage(study, "userProfileAttributes", "userProfileAttributes 'email' conflicts with existing user profile property");
    }
    
    @Test
    public void cannotAddConflictingExternalIdAttribute() {
        study.getUserProfileAttributes().add("externalId");
        assertCorrectMessage(study, "userProfileAttributes", "userProfileAttributes 'externalId' conflicts with existing user profile property");
    }
    
    @Test
    public void userProfileAttributesCannotStartWithDash() {
        study.getUserProfileAttributes().add("-illegal");
        assertCorrectMessage(study, "userProfileAttributes", "userProfileAttributes '-illegal' must contain only digits, letters, underscores and dashes, and cannot start with a dash");
    }
    
    @Test
    public void userProfileAttributesCannotContainSpaces() {
        study.getUserProfileAttributes().add("Game Points");
        assertCorrectMessage(study, "userProfileAttributes", "userProfileAttributes 'Game Points' must contain only digits, letters, underscores and dashes, and cannot start with a dash");
    }
    
    @Test
    public void userProfileAttributesCanBeJustADash() {
        study.getUserProfileAttributes().add("_");
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    @Test
    public void userProfileAttributesCanBeJustADashAndLetter() {
        study.getUserProfileAttributes().add("_A");
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    @Test
    public void userProfileAttributesCannotBeEmpty() {
        study.getUserProfileAttributes().add("");
        assertCorrectMessage(study, "userProfileAttributes", "userProfileAttributes '' must contain only digits, letters, underscores and dashes, and cannot start with a dash");
    }
    
    @Test
    public void requiresMissingConsentNotificationEmail() {
        study.setConsentNotificationEmail(null);
        assertCorrectMessage(study, "consentNotificationEmail", "consentNotificationEmail is required");
    }
    
    @Test
    public void requiresPasswordPolicy() {
        study.setPasswordPolicy(null);
        assertCorrectMessage(study, "passwordPolicy", "passwordPolicy is required");
    }
    
    @Test
    public void requiresVerifyEmailTemplate() {
        study.setVerifyEmailTemplate(null);
        assertCorrectMessage(study, "verifyEmailTemplate", "verifyEmailTemplate is required");
    }

    @Test
    public void requiresVerifyEmailTemplateWithSubject() {
        study.setVerifyEmailTemplate(new EmailTemplate("  ", "body", MimeType.HTML));
        assertCorrectMessage(study, "verifyEmailTemplate.subject", "verifyEmailTemplate.subject is required");
    }

    @Test
    public void requiresVerifyEmailTemplateWithBody() {
        study.setVerifyEmailTemplate(new EmailTemplate("subject", null, MimeType.HTML));
        assertCorrectMessage(study, "verifyEmailTemplate.body", "verifyEmailTemplate.body is required");
    }

    @Test
    public void requiresResetPasswordTemplate() {
        study.setResetPasswordTemplate(null);
        assertCorrectMessage(study, "resetPasswordTemplate", "resetPasswordTemplate is required");
    }

    @Test
    public void requiresResetPasswordTemplateWithSubject() {
        study.setResetPasswordTemplate(new EmailTemplate("  ", "body", MimeType.TEXT));
        assertCorrectMessage(study, "resetPasswordTemplate.subject", "resetPasswordTemplate.subject is required");
    }

    @Test
    public void requiresResetPasswordTemplateWithBody() {
        study.setResetPasswordTemplate(new EmailTemplate("subject", null, MimeType.TEXT));
        assertCorrectMessage(study, "resetPasswordTemplate.body", "resetPasswordTemplate.body is required");
    }
    
    @Test
    public void emailSignInTemplateNotRequired() {
        study.setEmailSignInTemplate(null);
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }

    @Test
    public void requiresEmailSignInTemplateWithSubject() {
        study.setEmailSignInTemplate(new EmailTemplate(null, "body", MimeType.HTML));
        assertCorrectMessage(study, "emailSignInTemplate.subject", "emailSignInTemplate.subject is required");
    }

    @Test
    public void requiresEmailSignInTemplateWithBody() {
        study.setEmailSignInTemplate(new EmailTemplate("subject", null, MimeType.HTML));
        assertCorrectMessage(study, "emailSignInTemplate.body", "emailSignInTemplate.body is required");
    }
    
    @Test
    public void requiresEmailSignInTemplateRequiresToken() {
        study.setEmailSignInTemplate(new EmailTemplate("subject", "body with no token", MimeType.HTML));
        assertCorrectMessage(study, "emailSignInTemplate.body", "emailSignInTemplate.body must contain the ${token} template variable");
    }
    
    @Test
    public void accountExistsTemplateNotRequired() {
        study.setAccountExistsTemplate(null);
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }

    @Test
    public void requiresAccountExistsTemplateWithSubject() {
        study.setAccountExistsTemplate(new EmailTemplate(null, "body", MimeType.HTML));
        assertCorrectMessage(study, "accountExistsTemplate.subject", "accountExistsTemplate.subject is required");
    }

    @Test
    public void requiresAccountExistsTemplateWithBody() {
        study.setAccountExistsTemplate(new EmailTemplate("subject", null, MimeType.HTML));
        assertCorrectMessage(study, "accountExistsTemplate.body", "accountExistsTemplate.body is required");
    }
    
    @Test
    public void requiresAccountExistsTemplateRequiresURL() {
        study.setAccountExistsTemplate(new EmailTemplate("subject", "body with no url", MimeType.HTML));
        assertCorrectMessage(study, "accountExistsTemplate.body", "accountExistsTemplate.body must contain the ${url} template variable");
    }
    
    @Test
    public void cannotSetMinAgeOfConsentLessThanZero() {
        study.setMinAgeOfConsent(-100);
        assertCorrectMessage(study, "minAgeOfConsent", "minAgeOfConsent must be zero (no minimum age of consent) or higher");
    }
    
    @Test
    public void cannotSetAccountLimitLessThanZero() {
        study.setAccountLimit(-100);
        assertCorrectMessage(study, "accountLimit", "accountLimit must be zero (no limit set) or higher");
    }
    
    @Test
    public void shortListOfDataGroupsOK() {
        study.setDataGroups(Sets.newHashSet("beta_users", "production_users", "testers", "internal"));
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    @Test
    public void longListOfDataGroupsInvalid() {
        study.setDataGroups(Sets.newTreeSet(Lists.newArrayList("Antwerp", "Ghent", "Charleroi", "Liege", "Brussels-City", "Bruges", "Schaerbeek", "Anderlecht", "Namur", "Leuven", "Mons", "Molenbeek-Saint-Jean")));
        assertCorrectMessage(study, "dataGroups", "dataGroups will not export to Synapse (string is over 100 characters: 'Anderlecht, Antwerp, Bruges, Brussels-City, Charleroi, Ghent, Leuven, Liege, Molenbeek-Saint-Jean, Mons, Namur, Schaerbeek')");
    }
    
    @Test
    public void dataGroupCharactersRestricted() {
        study.setDataGroups(Sets.newHashSet("Liège"));
        assertCorrectMessage(study, "dataGroups", "dataGroups contains invalid tag 'Liège' (only letters, numbers, underscore and dash allowed)");
    }

    @Test
    public void publicStudyWithoutExternalIdValidationIsValid() {
        study.setExternalIdValidationEnabled(false);
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    @Test
    public void publicStudyWithoutExternalIdOnSignUpIsValid() {
        study.setExternalIdRequiredOnSignup(false);
        Validate.entityThrowingException(StudyValidator.INSTANCE, study);
    }
    
    @Test
    public void nonPublicStudiesMustEnableExternalIdValdation() {
        study.setEmailVerificationEnabled(false);
        study.setExternalIdValidationEnabled(false);
        assertCorrectMessage(study, "externalIdValidationEnabled", "externalIdValidationEnabled cannot be disabled if email verification has been disabled");
    }
    
    @Test
    public void nonPublicStudiesMustRequireExternalIdOnSignUp() {
        study.setEmailVerificationEnabled(false);
        study.setExternalIdRequiredOnSignup(false);
        assertCorrectMessage(study, "externalIdRequiredOnSignup", "externalIdRequiredOnSignup cannot be disabled if email verification has been disabled");
    }    
}
