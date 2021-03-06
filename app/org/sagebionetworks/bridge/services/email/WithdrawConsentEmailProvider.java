package org.sagebionetworks.bridge.services.email;

import java.nio.charset.StandardCharsets;
import java.util.Set;

import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import org.sagebionetworks.bridge.BridgeUtils;
import org.sagebionetworks.bridge.models.accounts.Account;
import org.sagebionetworks.bridge.models.accounts.Withdrawal;
import org.sagebionetworks.bridge.models.studies.Study;

public class WithdrawConsentEmailProvider extends MimeTypeEmailProvider {
    
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("MMMM d, yyyy");
    private static final String CONSENT_EMAIL_SUBJECT = "Notification of consent withdrawal for %s";
    private static final String SUB_TYPE_HTML = "html";

    private String externalId;
    private Account account;
    private Withdrawal withdrawal;
    private long withdrewOn;
    
    public WithdrawConsentEmailProvider(Study study, String externalId, Account account, Withdrawal withdrawal, long withdrewOn) {
        super(study);
        this.externalId = externalId;
        this.account = account;
        this.withdrawal = withdrawal;
        this.withdrewOn = withdrewOn;
    }
    
    @Override
    public MimeTypeEmail getMimeTypeEmail() throws MessagingException {
        MimeTypeEmailBuilder builder = new MimeTypeEmailBuilder();

        String subject = String.format(CONSENT_EMAIL_SUBJECT, getStudy().getName());
        builder.withSubject(subject);

        final String sendFromEmail = getFormattedSenderEmail();
        builder.withSender(sendFromEmail);

        Set<String> emailAddresses = BridgeUtils.commaListToOrderedSet(getStudy().getConsentNotificationEmail());
        builder.withRecipients(emailAddresses);

        String content = String.format("<p>User %s withdrew from the study on %s. </p>", 
                getUserLabel(), FORMATTER.print(withdrewOn));
        content += "<p>Reason:</p>";
        if (StringUtils.isBlank(withdrawal.getReason())) {
            content += "<p><i>No reason given.</i></p>";
        } else {
            content += "<p>" + nullSafeCleanHtml(withdrawal.getReason()) + "</p>";
        }
        
        // Consent agreement as message body in HTML
        final MimeBodyPart bodyPart = new MimeBodyPart();
        bodyPart.setText(content, StandardCharsets.UTF_8.name(), SUB_TYPE_HTML);
        builder.withMessageParts(bodyPart);

        return builder.build();
    }
    
    private String getUserLabel() {
        String label = String.format("%s %s &lt;%s&gt;", nullSafeCleanHtml(account.getFirstName()),
                nullSafeCleanHtml(account.getLastName()), account.getEmail());
        if (externalId != null) {
            label += " (external ID: " + nullSafeCleanHtml(externalId) + ") ";
        }
        return label;
    }

    // Helper method to strip HTML from a string so it can be safely printed in the Withdraw Consent email. Converts
    // null or blank strings into an empty string, so the word "null" doesn't randomly appear in our emails.
    private static String nullSafeCleanHtml(String in) {
        if (StringUtils.isBlank(in)) {
            return "";
        } else {
            return Jsoup.clean(in, Whitelist.none());
        }
    }
}
