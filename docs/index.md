---
title: Home
layout: home
---

Documentation coming soon.

## REST API

Chimera provides a REST API for server administration. See the
[API Reference](api-reference) for a readable endpoint guide, or the
[interactive API docs](api.html) rendered from the OpenAPI spec.

Endpoints are supplied by explicitly configured shared libraries. See the
[REST module SDK](rest-module-sdk) for module configuration and building
independent extensions.

The API includes endpoints for:
- User management (`/api/core/v1/users`)
- NFS export management (`/api/core/v1/exports`)
- SMB share management (`/api/core/v1/shares`)
- S3 bucket management (`/api/core/v1/buckets`)
