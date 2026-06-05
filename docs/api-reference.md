---
title: API Reference
layout: default
nav_order: 4
permalink: /api-reference
---

# REST API Reference

Chimera exposes a REST API for server administration: managing builtin users, NFS
exports, SMB shares, and S3 buckets. This page documents every endpoint and its
arguments.

The same API is also available interactively:

- **Swagger UI** (live, served by the daemon): `http://<host>:8080/api/docs`
- **OpenAPI spec** (live): `http://<host>:8080/api/openapi.json`
- **ReDoc** (static): [api.html](api.html), rendered from
  [openapi.json](openapi.json)

## Conventions

| Property        | Value                                                       |
|-----------------|-------------------------------------------------------------|
| Base URL        | `http://<host>:8080/api/v1`                                 |
| Admin REST port | `8080` (see [Quick Start](quickstart))                      |
| Content type    | `application/json` for request and response bodies          |
| Authentication  | None (the admin API is unauthenticated)                     |
| Max request body| 65536 bytes for `POST` bodies                               |

### Status codes

| Code | Meaning                                                            |
|------|-------------------------------------------------------------------|
| 200  | OK (list / get)                                                   |
| 201  | Created (resource created)                                       |
| 204  | No Content (resource deleted)                                    |
| 400  | Bad Request (malformed JSON or missing required field)           |
| 404  | Not Found (unknown resource or unknown path)                     |
| 405  | Method Not Allowed (valid path, unsupported method)              |
| 500  | Internal Server Error (backend failed to create the resource)    |

### Error response

All errors return a JSON body of the form:

```json
{
  "error": "Bad Request",
  "message": "Missing required field: username"
}
```

Success responses for create operations return:

```json
{
  "message": "User created"
}
```

---

## Users

Users are stored in the VFS user cache. Those created through this API are "pinned"
(builtin) and reported with `"pinned": true`.

### List users

```
GET /api/v1/users
```

Returns an array of user objects.

**Response `200`**

```json
[
  {
    "username": "alice",
    "uid": 1000,
    "gid": 1000,
    "pinned": true,
    "gids": [1000, 27, 44]
  }
]
```

| Field      | Type           | Description                          |
|------------|----------------|--------------------------------------|
| `username` | string         | User login name                      |
| `uid`      | integer        | User ID                              |
| `gid`      | integer        | Primary group ID                     |
| `pinned`   | boolean        | Whether the user is builtin (pinned) |
| `gids`     | array<integer> | Supplementary group IDs              |

```bash
curl http://localhost:8080/api/v1/users
```

### Get user

```
GET /api/v1/users/{username}
```

| Path parameter | Type   | Description           |
|----------------|--------|-----------------------|
| `username`     | string | Name of the user      |

**Response `200`** - a single user object (same fields as the list above).

**Errors:** `404` if the user does not exist.

```bash
curl http://localhost:8080/api/v1/users/alice
```

### Create user

```
POST /api/v1/users
```

**Request body**

| Field       | Type           | Required | Description                                              |
|-------------|----------------|----------|----------------------------------------------------------|
| `username`  | string         | yes      | User login name                                          |
| `password`  | string         | no       | User password                                            |
| `smbpasswd` | string         | no       | SMB/NTLM password hash                                   |
| `uid`       | integer        | no       | User ID (defaults to `0` if omitted)                     |
| `gid`       | integer        | no       | Primary group ID (defaults to `0` if omitted)            |
| `gids`      | array<integer> | no       | Supplementary group IDs; at most 64 (more than 64 returns `400`) |

**Response `201`**

```json
{ "message": "User created" }
```

**Errors:** `400` (invalid JSON, missing `username`, or more than 64 `gids`),
`500` (creation failed).

```bash
curl -X POST http://localhost:8080/api/v1/users \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","uid":1000,"gid":1000,"gids":[1000,27,44]}'
```

### Delete user

```
DELETE /api/v1/users/{username}
```

| Path parameter | Type   | Description      |
|----------------|--------|------------------|
| `username`     | string | Name of the user |

**Response `204`** - no body.

**Errors:** `404` if the user does not exist.

```bash
curl -X DELETE http://localhost:8080/api/v1/users/alice
```

---

## NFS Exports

NFS exports map an export name to a VFS path served over NFSv3/NFSv4.

### List exports

```
GET /api/v1/exports
```

**Response `200`**

```json
[
  { "name": "export", "path": "/memfs/export" }
]
```

| Field  | Type   | Description |
|--------|--------|-------------|
| `name` | string | Export name |
| `path` | string | VFS path    |

```bash
curl http://localhost:8080/api/v1/exports
```

### Get export

```
GET /api/v1/exports/{name}
```

| Path parameter | Type   | Description     |
|----------------|--------|-----------------|
| `name`         | string | Name of export  |

**Response `200`** - a single export object (`name`, `path`).

**Errors:** `404` if the export does not exist.

```bash
curl http://localhost:8080/api/v1/exports/export
```

### Create export

```
POST /api/v1/exports
```

**Request body**

| Field  | Type   | Required | Description |
|--------|--------|----------|-------------|
| `name` | string | yes      | Export name |
| `path` | string | yes      | VFS path    |

**Response `201`**

```json
{ "message": "Export created" }
```

**Errors:** `400` (invalid JSON, or missing `name`/`path`), `500` (creation failed).

```bash
curl -X POST http://localhost:8080/api/v1/exports \
  -H "Content-Type: application/json" \
  -d '{"name":"export","path":"/memfs/export"}'
```

### Delete export

```
DELETE /api/v1/exports/{name}
```

| Path parameter | Type   | Description    |
|----------------|--------|----------------|
| `name`         | string | Name of export |

**Response `204`** - no body.

**Errors:** `404` if the export does not exist.

```bash
curl -X DELETE http://localhost:8080/api/v1/exports/export
```

---

## SMB Shares

SMB shares map a share name to a VFS path served over SMB2/SMB3.

### List shares

```
GET /api/v1/shares
```

**Response `200`**

```json
[
  { "name": "export", "path": "/memfs/export" }
]
```

| Field  | Type   | Description |
|--------|--------|-------------|
| `name` | string | Share name  |
| `path` | string | VFS path    |

```bash
curl http://localhost:8080/api/v1/shares
```

### Get share

```
GET /api/v1/shares/{name}
```

| Path parameter | Type   | Description    |
|----------------|--------|----------------|
| `name`         | string | Name of share  |

**Response `200`** - a single share object (`name`, `path`).

**Errors:** `404` if the share does not exist.

```bash
curl http://localhost:8080/api/v1/shares/export
```

### Create share

```
POST /api/v1/shares
```

**Request body**

| Field  | Type   | Required | Description |
|--------|--------|----------|-------------|
| `name` | string | yes      | Share name  |
| `path` | string | yes      | VFS path    |

**Response `201`**

```json
{ "message": "Share created" }
```

**Errors:** `400` (invalid JSON, or missing `name`/`path`), `500` (creation failed).

```bash
curl -X POST http://localhost:8080/api/v1/shares \
  -H "Content-Type: application/json" \
  -d '{"name":"export","path":"/memfs/export"}'
```

### Delete share

```
DELETE /api/v1/shares/{name}
```

| Path parameter | Type   | Description   |
|----------------|--------|---------------|
| `name`         | string | Name of share |

**Response `204`** - no body.

**Errors:** `404` if the share does not exist.

```bash
curl -X DELETE http://localhost:8080/api/v1/shares/export
```

---

## S3 Buckets

S3 buckets map a bucket name to a VFS path served over the S3-compatible API.

### List buckets

```
GET /api/v1/buckets
```

**Response `200`**

```json
[
  { "name": "export", "path": "/memfs/export" }
]
```

| Field  | Type   | Description |
|--------|--------|-------------|
| `name` | string | Bucket name |
| `path` | string | VFS path    |

```bash
curl http://localhost:8080/api/v1/buckets
```

### Get bucket

```
GET /api/v1/buckets/{name}
```

| Path parameter | Type   | Description     |
|----------------|--------|-----------------|
| `name`         | string | Name of bucket  |

**Response `200`** - a single bucket object (`name`, `path`).

**Errors:** `404` if the bucket does not exist.

```bash
curl http://localhost:8080/api/v1/buckets/export
```

### Create bucket

```
POST /api/v1/buckets
```

**Request body**

| Field  | Type   | Required | Description |
|--------|--------|----------|-------------|
| `name` | string | yes      | Bucket name |
| `path` | string | yes      | VFS path    |

**Response `201`**

```json
{ "message": "Bucket created" }
```

**Errors:** `400` (invalid JSON, or missing `name`/`path`), `500` (creation failed).

```bash
curl -X POST http://localhost:8080/api/v1/buckets \
  -H "Content-Type: application/json" \
  -d '{"name":"export","path":"/memfs/export"}'
```

### Delete bucket

```
DELETE /api/v1/buckets/{name}
```

| Path parameter | Type   | Description    |
|----------------|--------|----------------|
| `name`         | string | Name of bucket |

**Response `204`** - no body.

**Errors:** `404` if the bucket does not exist.

```bash
curl -X DELETE http://localhost:8080/api/v1/buckets/export
```

---

## Utility endpoints

These endpoints live at the server root rather than under `/api/v1`.

### Version

```
GET /version
```

**Response `200`**

```json
{ "version": "0.1.0" }
```

```bash
curl http://localhost:8080/version
```

### OpenAPI specification

```
GET /api/openapi.json
```

Returns the raw OpenAPI 3.0 document describing the `/api/v1` API.

### Swagger UI

```
GET /api/docs
```

Serves the interactive Swagger UI (with its bundled JS/CSS assets under
`/api/docs/`).

### Global behavior

- Any unrecognized path returns `404` with `{"error":"Not Found"}`.
- A recognized path with an unsupported method returns `405` with
  `{"error":"Method Not Allowed"}`.
