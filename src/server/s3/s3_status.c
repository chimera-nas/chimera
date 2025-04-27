#include <stdio.h>
#include "s3_status.h"
#include "s3_internal.h"

const char *
chimera_s3_status_to_string(enum chimera_s3_status status)
{
    switch (status) {
        case CHIMERA_S3_STATUS_OK:
            return "OK";
        case CHIMERA_S3_STATUS_NOT_FOUND:
            return "Not Found";
        case CHIMERA_S3_STATUS_ACCESS_DENIED:
            return "Access Denied";
        case CHIMERA_S3_STATUS_BAD_REQUEST:
            return "Bad Request";
        case CHIMERA_S3_STATUS_INTERNAL_ERROR:
            return "Internal Error";
        case CHIMERA_S3_STATUS_REQUEST_TIMEOUT:
            return "Request Timeout";
        case CHIMERA_S3_STATUS_NO_SUCH_BUCKET:
            return "No Such Bucket";
        case CHIMERA_S3_STATUS_NO_SUCH_KEY:
            return "No Such Key";
        default:
            return "Internal Error";
    } /* switch */
} /* chimera_s3_status_to_string */

int
chimera_s3_prepare_error_response(
    struct chimera_s3_request *request,
    char                      *buffer,
    int                       *length)
{
    char *bp   = buffer;
    int   code = 500;

    bp += sprintf(bp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    bp += sprintf(bp, "<Error>\n");

    switch (request->status) {
        case CHIMERA_S3_STATUS_NO_SUCH_BUCKET:
            bp  += sprintf(bp, "  <Code>NoSuchBucket</Code>\n");
            bp  += sprintf(bp, "  <Message>The specified bucket does not exist.</Message>\n");
            code = 404;
            break;
        case CHIMERA_S3_STATUS_NO_SUCH_KEY:
            bp  += sprintf(bp, "  <Code>NoSuchKey</Code>\n");
            bp  += sprintf(bp, "  <Message>The specified key does not exist.</Message>\n");
            code = 404;
            break;
        case CHIMERA_S3_STATUS_ACCESS_DENIED:
            bp  += sprintf(bp, "  <Code>AccessDenied</Code>\n");
            bp  += sprintf(bp, "  <Message>Access Denied</Message>\n");
            code = 403;
            break;
        default:
            bp  += sprintf(bp, "  <Code>InternalError</Code>\n");
            bp  += sprintf(bp, "  <Message>Internal Error</Message>\n");
            code = 500;
            break;
    } /* switch */

    bp += sprintf(bp, "  <Resource>%s</Resource>\n", request->path);
    bp += sprintf(bp, "  <RequestId>4442587FB7D0A2F9</RequestId>\n");
    bp += sprintf(bp, "  <HostId>MyMagicHostId=</HostId>\n");
    bp += sprintf(bp, "</Error>\n");

    *length = bp - buffer;

    return code;
} /* chimera_s3_prepare_error_response */
