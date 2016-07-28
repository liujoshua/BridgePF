package org.sagebionetworks.bridge.models.upload;

/**
 * For informational purposes, we record the means through which an upload is marked
 * as completed and available for export. We have a couple of paths to make this happen
 * at this point in time.
 */
public enum UploadCompletionClient {
    /**
     * Upload has been completed by a call from the application to the REST API.
     */
    APP,
    /**
     * Upload has been completed by a worker process that listens for the addition 
     * of upload files to the file upload bucket on S3.
     */
    S3_WORKER,
    /**
     * Upload has been completed by a process that scans uploaded files for ones which
     * need to be marked complete, i.e. uploads where the data is in S3 but Bridge's
     * Upload Complete handler was not notified.
     */
    S3_SWEEPER
}
