#pragma once

struct chimera_s3_request;

enum chimera_s3_status {
    CHIMERA_S3_STATUS_OK,
    CHIMERA_S3_STATUS_NOT_FOUND,
    CHIMERA_S3_STATUS_NOT_IMPLEMENTED,
    CHIMERA_S3_STATUS_BAD_REQUEST,
    CHIMERA_S3_STATUS_INTERNAL_ERROR,
    CHIMERA_S3_STATUS_ACCESS_DENIED,
    CHIMERA_S3_STATUS_PRECONDITION_FAILED,
    CHIMERA_S3_STATUS_REQUEST_TIMEOUT,
    CHIMERA_S3_STATUS_NO_SUCH_BUCKET,
    CHIMERA_S3_STATUS_NO_SUCH_KEY,
};

const char *
chimera_s3_status_to_string(
    enum chimera_s3_status status);

int
chimera_s3_prepare_error_response(
    struct chimera_s3_request *request,
    char                      *buffer,
    int                       *length);