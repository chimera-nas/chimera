#include <stdio.h>

#include "s3_internal.h"
#include "evpl/evpl_http.h"

void
chimera_s3_dump_request(struct chimera_s3_request *request)
{
    char extra[80];

    extra[0] = '\0';

    switch (evpl_http_request_type(request->http_request)) {
        case EVPL_HTTP_REQUEST_TYPE_GET:
            snprintf(extra, sizeof(extra), "offset %lu length %lu", request->file_offset, request->file_left);
            break;
        default:
            break;
    } /* switch */

    chimera_s3_debug("S3   Request %p: %s %.*s:%s %s",
                     request,
                     evpl_http_request_type_to_string(request->http_request),
                     request->bucket_namelen,
                     request->bucket_name,
                     request->path,
                     extra);
} /* chimera_s3_dump_request */

void
chimera_s3_dump_response(struct chimera_s3_request *request)
{
    chimera_s3_debug("S3   Reply   %p: %s %.*s:%s -> (%s) elapsed %lunS",
                     request,
                     evpl_http_request_type_to_string(request->http_request),
                     request->bucket_namelen,
                     request->bucket_name,
                     request->path,
                     chimera_s3_status_to_string(request->status),
                     request->elapsed);
} /* chimera_s3_dump_response */



