#pragma once

#include <stdint.h>
#include <stddef.h>

static inline int
chimera_decode_uint64(
    const uint8_t *buffer,
    uint64_t      *value)
{
    int      i      = 0;
    uint64_t result = 0;
    int      shift  = 0;
    uint8_t  byte;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint64_t) (byte & 0x7F) << shift;

 done:
    *value = result;
    return i;
} /* chimera_decode_uint64 */

static inline int
chimera_decode_uint32(
    const uint8_t *buffer,
    uint32_t      *value)
{
    int      i      = 0;
    uint32_t result = 0;
    int      shift  = 0;
    uint8_t  byte;

    byte    = buffer[i++];
    result |= (uint32_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint32_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint32_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint32_t) (byte & 0x7F) << shift;
    if (!(byte & 0x80)) {
        goto done;
    }
    shift += 7;

    byte    = buffer[i++];
    result |= (uint32_t) (byte & 0x7F) << shift;

 done:
    *value = result;
    return i;
} /* chimera_decode_uint32 */

static inline int
chimera_encode_uint64(
    uint64_t value,
    uint8_t *buffer)
{
    int i = 0;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);

 done:
    i++;
    return i;
} /* chimera_encode_uint64 */

static inline int
chimera_encode_uint32(
    uint32_t value,
    uint8_t *buffer)
{
    int i = 0;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);
    if (value <= 0x7F) {
        goto done;
    }
    buffer[i++] |= 0x80;
    value      >>= 7;

    buffer[i] = (uint8_t) (value & 0x7F);

 done:
    i++;
    return i;
} /* chimera_encode_uint32 */

