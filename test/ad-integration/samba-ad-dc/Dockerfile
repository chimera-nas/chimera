# SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: LGPL-2.1-only

# Samba AD DC container for integration testing
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    samba \
    samba-dsdb-modules \
    samba-vfs-modules \
    winbind \
    krb5-user \
    krb5-kdc \
    dnsutils \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Copy provisioning script
COPY provision.sh /provision.sh
RUN chmod +x /provision.sh

EXPOSE 53/tcp 53/udp 88/tcp 88/udp 135/tcp 139/tcp 389/tcp 389/udp 445/tcp 464/tcp 464/udp 636/tcp 3268/tcp 3269/tcp

ENTRYPOINT ["/provision.sh"]
CMD ["samba", "-F", "--no-process-group"]
