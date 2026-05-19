// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

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
        case CHIMERA_S3_STATUS_INVALID_ACCESS_KEY_ID:
            return "Invalid Access Key Id";
        case CHIMERA_S3_STATUS_SIGNATURE_MISMATCH:
            return "Signature Does Not Match";
        case CHIMERA_S3_STATUS_MISSING_AUTH_HEADER:
            return "Missing Security Header";
        case CHIMERA_S3_STATUS_NO_SUCH_UPLOAD:
            return "No Such Upload";
        case CHIMERA_S3_STATUS_INVALID_PART:
            return "Invalid Part";
        case CHIMERA_S3_STATUS_INVALID_PART_ORDER:
            return "Invalid Part Order";
        case CHIMERA_S3_STATUS_INVALID_PART_NUMBER:
            return "Invalid Part Number";
        case CHIMERA_S3_STATUS_ENTITY_TOO_SMALL:
            return "Entity Too Small";
        case CHIMERA_S3_STATUS_MALFORMED_XML:
            return "Malformed XML";
        case CHIMERA_S3_STATUS_NO_CONTENT:
            return "No Content";
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
        case CHIMERA_S3_STATUS_INVALID_ACCESS_KEY_ID:
            bp += sprintf(bp, "  <Code>InvalidAccessKeyId</Code>\n");
            bp += sprintf(bp,
                          "  <Message>The AWS Access Key Id you provided does not exist in our records.</Message>\n");
            code = 403;
            break;
        case CHIMERA_S3_STATUS_SIGNATURE_MISMATCH:
            bp += sprintf(bp, "  <Code>SignatureDoesNotMatch</Code>\n");
            bp += sprintf(bp,
                          "  <Message>The request signature we calculated does not match the signature you provided.</Message>\n");
            code = 403;
            break;
        case CHIMERA_S3_STATUS_MISSING_AUTH_HEADER:
            bp  += sprintf(bp, "  <Code>MissingSecurityHeader</Code>\n");
            bp  += sprintf(bp, "  <Message>Your request is missing a required header.</Message>\n");
            code = 400;
            break;
        case CHIMERA_S3_STATUS_NO_SUCH_UPLOAD:
            bp  += sprintf(bp, "  <Code>NoSuchUpload</Code>\n");
            bp  += sprintf(bp, "  <Message>The specified upload does not exist.</Message>\n");
            code = 404;
            break;
        case CHIMERA_S3_STATUS_INVALID_PART:
            bp  += sprintf(bp, "  <Code>InvalidPart</Code>\n");
            bp  += sprintf(bp, "  <Message>One or more of the specified parts could not be found.</Message>\n");
            code = 400;
            break;
        case CHIMERA_S3_STATUS_INVALID_PART_ORDER:
            bp += sprintf(bp, "  <Code>InvalidPartOrder</Code>\n");
            bp += sprintf(bp,
                          "  <Message>The list of parts was not in ascending order. Parts must be in ascending order by part number.</Message>\n");
            code = 400;
            break;
        case CHIMERA_S3_STATUS_INVALID_PART_NUMBER:
            bp += sprintf(bp, "  <Code>InvalidArgument</Code>\n");
            bp += sprintf(bp,
                          "  <Message>Part number must be an integer between 1 and 10000, inclusive.</Message>\n");
            code = 400;
            break;
        case CHIMERA_S3_STATUS_ENTITY_TOO_SMALL:
            bp += sprintf(bp, "  <Code>EntityTooSmall</Code>\n");
            bp += sprintf(bp,
                          "  <Message>Your proposed upload is smaller than the minimum allowed object size. Each part must be at least 5 MB in size, except the last part.</Message>\n");
            code = 400;
            break;
        case CHIMERA_S3_STATUS_MALFORMED_XML:
            bp += sprintf(bp, "  <Code>MalformedXML</Code>\n");
            bp += sprintf(bp,
                          "  <Message>The XML you provided was not well-formed or did not validate against our published schema.</Message>\n");
            code = 400;
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
