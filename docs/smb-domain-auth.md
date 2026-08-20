---
title: SMB Domain Authentication
layout: default
nav_order: 5
permalink: /smb-domain-auth
---

# SMB Domain Authentication
{: .no_toc }

Chimera can authenticate SMB clients against a directory instead of the
built-in `users` list in its config file. This page walks through the two
setups that are regularly exercised against chimera:

- an **Active Directory** domain, joined with `adcli`, with winbind providing
  NTLM pass-through and a keytab providing Kerberos;
- a plain **OpenLDAP** directory extended with the Samba schema, used as
  samba's `ldapsam` passdb and served to chimera through winbind.

Both arrive at the same place: `winbindd` is the component that validates
credentials and maps them to POSIX identities, and chimera talks to
`winbindd`. Read the first section before either procedure; it explains why
the two configurations differ so little on the chimera side, and lists the
requirements that are easy to miss.

1. TOC
{:toc}

---

## How chimera authenticates SMB users

Chimera has three ways to authenticate an SMB session, tried in this order:

| Path | Credential source | When it applies |
|---|---|---|
| Built-in users | The `users` array in the chimera config (an `smbpasswd` NT hash) | Always tried first. NTLMv2 is verified in-process, with no external daemon. |
| NTLM pass-through | `winbindd` | The account is not a built-in user and `winbind_enabled` is set. |
| Kerberos / GSSAPI | A keytab on the chimera host | The client selects Kerberos through SPNEGO and `kerberos_enabled` is set. |

Chimera never speaks LDAP, and never joins a domain or maintains a netlogon
channel of its own. Its SMB server links against `libwbclient` and asks
`winbindd` to validate every pass-through logon, so any identity source
winbindd can serve works with no chimera-side support. That is why the chimera
configuration in the two procedures below is nearly identical: only
`winbind_domain` and the Kerberos keys differ. Everything that is specific to
Active Directory or to LDAP is configured in samba and winbind.

The POSIX uid and gid that chimera stamps on files created over SMB come from
winbind's identity mapping, so the idmap backend you choose decides on-disk
ownership.

### Configuration keys

The chimera side of both procedures is the `server.smb_auth` object:

| Key | Type | Default | Description |
|---|---|---|---|
| `winbind_enabled` | bool | `false` | Authenticate through winbindd, and register winbind as a SID/uid resolver for SMB security descriptors. |
| `winbind_domain` | string | - | Fallback NetBIOS domain advertised in the NTLM CHALLENGE if winbindd cannot be queried for the join identity at startup. |
| `kerberos_enabled` | bool | `false` | Accept Kerberos through SPNEGO. |
| `kerberos_keytab` | string | - | Keytab used to accept Kerberos contexts. If unset, the MIT default (or `KRB5_KTNAME`) applies. |
| `kerberos_realm` | string | - | Realm the host belongs to, recorded at startup. |

See [server.smb_auth](configuration#serversmb_auth) in the configuration
reference for how these sit in the wider config file.

### Requirements

**Chimera must be built with libwbclient.** It is an optional build
dependency, detected with `pkg-config`, so cmake reports either
`libwbclient found - Winbind integration enabled` or
`libwbclient not found - Winbind integration disabled`. In the second case the
resulting binary accepts `winbind_enabled: true` but does nothing with it.
Install `libwbclient-dev` (Debian/Ubuntu) or `libwbclient-devel` (RHEL/Rocky)
and reconfigure. The prebuilt container images already include it.

**Chimera needs the privileged winbindd pipe.** NTLM pass-through uses
winbindd's challenge/response interface, which samba serves only on the
privileged socket under `/var/lib/samba/winbindd_privileged` (mode 0750,
root-owned). Run chimera as root, or give it membership of the group that owns
that directory.

**winbindd must be running and joined before chimera starts.** Chimera
resolves the NetBIOS computer and domain names it advertises in its NTLM
CHALLENGE once, at startup, and caches them for the life of the process.
Domain controllers validate the target info a client echoes back against the
machine account on the netlogon channel, so if chimera starts before the host
is joined it advertises fallback names and every pass-through logon fails with
`NT_STATUS_LOGON_FAILURE`. There is no retry: restart chimera after a join or
a rejoin.

**Enable winbind even for a Kerberos-only deployment.** The Kerberos path maps
the authenticated principal to a uid through winbind. With `winbind_enabled`
off, or if the mapping fails, the session silently falls back to uid and gid
65534, and every user shares one identity on disk.

**`smbd` must not be running.** Chimera binds port 445 itself. If samba's file
server is installed as a side effect of another package, disable and mask it.

> **Note:** chimera's SPNEGO NEGOTIATE response advertises Kerberos
> unconditionally, including when `kerberos_enabled` is `false`. A client that
> prefers Kerberos will select it and its logon will then be rejected. Where
> only NTLM is configured, point clients at NTLM explicitly (for `smbclient`,
> `--use-kerberos=off`).

> **Note:** the uid and gid resolved for a session are applied to files it
> creates. The supplementary group list winbind returns is retrieved but is not
> applied to the credential used for VFS operations, so do not rely on
> secondary group membership for access decisions.

## Prerequisites

For either procedure you need a host with:

- **Working DNS.** The domain and the directory host must resolve. For Active
  Directory, `getent hosts example.com` must return the domain controllers.
- **Clock in sync.** Kerberos rejects a skew of more than a few minutes, which
  breaks the domain join long before it breaks a logon. Check with
  `timedatectl status`.
- **The packages below.**

| Purpose | Debian / Ubuntu | RHEL / Rocky |
|---|---|---|
| winbind daemon and NSS module | `winbind libnss-winbind` | `samba-winbind samba-winbind-clients` |
| samba command-line tools | `samba-common-bin` | `samba-common-tools` |
| Active Directory join | `adcli krb5-user tdb-tools` | `adcli krb5-workstation` |
| OpenLDAP server and tools | `slapd ldap-utils smbldap-tools` | `openldap-servers openldap-clients` |
| Client used to test | `smbclient` | `samba-client` |
| Build from source | `libwbclient-dev libkrb5-dev` | `libwbclient-devel krb5-devel` |

The commands below are written for Debian and Ubuntu, which is where they are
regularly exercised. On RHEL-family systems the equivalents differ in two
places worth knowing about: `/etc/nsswitch.conf` is generated by
`authselect`, so use `authselect select winbind --force` rather than editing
the file; and `smbldap-tools` is not in the base repositories, so the
directory in the second procedure has to be populated from your own LDIF
instead.

> **Warning:** for the Active Directory procedure, install the winbind
> packages but **not** `samba`. The `samba` package brings in `smbd`, which
> would hold port 445 against chimera. The second procedure does need `samba`,
> and disables `smbd` explicitly.

## Active Directory with winbind and Kerberos

This joins the host to an AD domain as an ordinary member server. Winbind
validates NTLM logons against a domain controller, and the keytab written by
the join lets chimera accept Kerberos.

The worked example uses these values:

| Role | Value |
|---|---|
| AD DNS domain | `example.com` |
| Kerberos realm | `EXAMPLE.COM` |
| NetBIOS workgroup | `EXAMPLE` |
| Account allowed to join | `Administrator` |
| Domain user used for testing | `alice` |

### 1. Install the packages

```bash
apt update
DEBIAN_FRONTEND=noninteractive apt install -y \
    winbind libnss-winbind libpam-winbind \
    adcli krb5-user samba-common-bin tdb-tools \
    smbclient
```

Confirm the domain is reachable before going further:

```bash
timedatectl status
getent hosts example.com
adcli info example.com
```

### 2. Configure Kerberos

```bash
cat > /etc/krb5.conf <<'EOF'
[libdefaults]
    default_realm = EXAMPLE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = true
    rdns = false

[domain_realm]
    .example.com = EXAMPLE.COM
    example.com = EXAMPLE.COM
EOF
```

`dns_lookup_kdc = true` is what lets the join and `kinit` find a live domain
controller through SRV records. `rdns = false` keeps service principal names
from being built out of reverse DNS, which is a common source of
`Server not found in Kerberos database` on hosts with imperfect PTR records.

### 3. Configure winbind

```bash
cat > /etc/samba/smb.conf <<'EOF'
[global]
    security = ads
    realm = EXAMPLE.COM
    workgroup = EXAMPLE
    kerberos method = secrets and keytab

    winbind use default domain = yes
    winbind refresh tickets = yes
    winbind enum users = yes
    winbind enum groups = yes
    template shell = /bin/bash

    idmap config * : backend = tdb
    idmap config * : range = 3000-7999
    idmap config EXAMPLE : backend = rid
    idmap config EXAMPLE : range = 100000-999999
EOF
testparm -s
```

The `rid` backend derives each uid arithmetically from the RID in the user's
SID, so the mapping is reproducible: several chimera nodes serving the same
filesystem agree on ownership without sharing an idmap database. The `*`
range covers SIDs from outside the domain and must not overlap the domain
range.

`winbind use default domain = yes` lets clients authenticate as `alice`
rather than `EXAMPLE\alice`.

### 4. Seed the samba machine password records

```bash
tdbtool /var/lib/samba/private/secrets.tdb \
    store SECRETS/MACHINE_PASSWORD/EXAMPLE 'bootstrap\00'
tdbtool /var/lib/samba/private/secrets.tdb \
    store SECRETS/MACHINE_LAST_CHANGE_TIME/EXAMPLE '\01\00\00\00'
```

This step is needed on samba 4.16 and newer. `adcli join --add-samba-data`
stores the machine password by calling `net changesecretpw`, which can only
*upgrade* an existing legacy record: on a host that has never been joined
there is nothing to upgrade and the call fails. The join then overwrites these
placeholders with the real machine password.

Without the machine password in `secrets.tdb`, winbindd cannot establish its
netlogon channel, `wbinfo --ping-dc` fails, and every domain logon fails. The
keytab alone is not enough.

### 5. Join the domain

```bash
echo '<join-password>' | adcli join \
    --domain example.com \
    --login-user Administrator \
    --service-name=cifs \
    --dont-expire-password=true \
    --ldap-passwd \
    --stdin-password \
    --add-samba-data \
    --verbose
```

Three of those flags matter more than they look:

- **`--domain`, not `--domain-controller`.** Given a domain name, adcli
  discovers a controller and talks to it under its real hostname. Pointing
  `--domain-controller` at a name that is a round-robin alias for several
  controllers, and so has no `ldap/` service principal of its own, makes the
  GSSAPI bind fail intermittently with `Message stream modified`.
- **`--service-name=cifs`** adds a `cifs/` principal to the keytab. That is
  the service name SMB clients ask for, so it is what chimera needs in order
  to accept Kerberos.
- **`--ldap-passwd`** sets the machine password over LDAP rather than through
  kpasswd. A kpasswd change that cannot be verified can leave the keytab out
  of step with the account even though the password did change.

Check that the join wrote both the keytab and the samba records:

```bash
klist -k /etc/krb5.keytab
tdbdump /var/lib/samba/private/secrets.tdb | grep key
```

### 6. Point NSS at winbind and start it

```bash
sed -i -E 's/^(passwd|group):(.*)/\1:\2 winbind/' /etc/nsswitch.conf
grep -E '^(passwd|group):' /etc/nsswitch.conf

systemctl enable --now winbind
```

A freshly created machine password takes time to replicate to every domain
controller, so winbindd may first contact one that does not know it yet and
drop into offline mode. Restarting winbindd makes it pick a controller again
and retry immediately:

```bash
until wbinfo -i alice >/dev/null 2>&1; do
    sleep 3
    systemctl restart winbind
    sleep 3
done
```

Then verify the whole path, from the netlogon channel down to NSS:

```bash
wbinfo --ping-dc
wbinfo -u | head
wbinfo -g | head
ntlm_auth --username=alice --domain=EXAMPLE --password='<password>'
getent passwd alice
id alice
```

`getent passwd alice` must return a uid inside the `rid` range configured
above. If it returns nothing while `wbinfo -u` lists the user, NSS is not
consulting winbind.

### 7. Configure chimera

In `/usr/local/etc/chimera.json`:

```json
{
    "server": {
        "smb_enabled": true,
        "smb_auth": {
            "winbind_enabled": true,
            "winbind_domain": "EXAMPLE",
            "kerberos_enabled": true,
            "kerberos_keytab": "/etc/krb5.keytab",
            "kerberos_realm": "EXAMPLE.COM"
        }
    },
    "mounts": { "data": { "module": "linux", "path": "/srv/chimera/data" } },
    "shares": { "smb1": { "path": "/data" } }
}
```

`mounts` names a VFS backend and `shares` publishes it; the `/data` in the
share refers to the chimera namespace, not the host filesystem. See
[Configuration](configuration) for the rest of the file.

Start chimera only once winbind is healthy:

```bash
mkdir -p /srv/chimera/data
chimera -c /usr/local/etc/chimera.json
```

If you run chimera under a service manager, order it after the winbind
service, and restart it after any later rejoin.

### 8. Test

NTLM, domain-qualified and bare:

```bash
smbclient --use-kerberos=off -U 'EXAMPLE/alice%<password>' \
    //127.0.0.1/smb1 -c 'allinfo /'
smbclient --use-kerberos=off -U 'alice%<password>' \
    //127.0.0.1/smb1 -c 'allinfo /'
```

Kerberos, from a ticket cache and from a password:

```bash
echo '<password>' | kinit alice -c /tmp/alice.ccache
klist -c /tmp/alice.ccache

smbclient --use-kerberos=required --use-krb5-ccache=/tmp/alice.ccache \
    -U alice //"$(hostname -f)"/smb1 -c 'allinfo /'

smbclient --use-kerberos=required -U 'alice%<password>' \
    //"$(hostname -f)"/smb1 -c 'allinfo /'
```

The Kerberos tests must address the host by name, not `127.0.0.1`: the client
builds the service principal it asks for out of the name you give it, and only
the real hostname matches the `cifs/` principal the join registered.

Finally, confirm that a file written over SMB lands on disk owned by the
mapped domain identity:

```bash
smbclient --use-kerberos=off -U 'alice%<password>' //127.0.0.1/smb1 \
    -c 'put /etc/hostname adtest.txt'
stat -c '%U %G %n' /srv/chimera/data/adtest.txt
```

The owner must come back as `alice`, resolved through winbind's NSS module. If
it prints a bare number instead, NSS cannot resolve the uid that chimera
stamped, which usually means `nsswitch.conf` was not updated.

## OpenLDAP with the Samba schema

This second procedure serves a local SAM domain whose password database lives
in an OpenLDAP directory: `slapd` holds the accounts, samba's `ldapsam` passdb
reads them, and winbindd authenticates against that passdb. It suits sites
that already run LDAP for POSIX identities and want SMB authentication from
the same directory without deploying Active Directory.

> **Note:** this is an NTLM-only deployment. An OpenLDAP directory has no KDC,
> so leave `kerberos_enabled` off. Chimera's Kerberos path resolves principals
> through winbind with no fallback, so enabling it without a KDC would map
> every session to uid 65534.

The worked example uses these values:

| Role | Value |
|---|---|
| DNS domain slapd is configured for | `example.com` |
| Directory suffix | `dc=example,dc=com` |
| Directory rootDN | `cn=admin,dc=example,dc=com` |
| LDAP URI | `ldap://127.0.0.1` |
| NetBIOS workgroup | `EXAMPLE` |
| POSIX and samba group | `nasusers` |
| User used for testing | `alice` |

### 1. Install the packages and stop smbd

Preseed slapd first so the install creates the directory with the suffix you
want:

```bash
cat > /tmp/slapd.preseed <<'EOF'
slapd slapd/no_configuration boolean false
slapd slapd/domain string example.com
slapd shared/organization string example.com
slapd slapd/password1 password <ldap-admin-password>
slapd slapd/password2 password <ldap-admin-password>
slapd slapd/purge_database boolean true
slapd slapd/move_old_database boolean true
slapd slapd/allow_ldap_v2 boolean false
EOF
debconf-set-selections /tmp/slapd.preseed

apt update
DEBIAN_FRONTEND=noninteractive apt install -y \
    slapd ldap-utils smbldap-tools \
    samba samba-common-bin winbind libnss-winbind \
    smbclient
```

The `samba` package is needed here for `pdbedit` and for the Samba schema file
it ships, but it also starts `smbd`. Stop it and confirm port 445 is free:

```bash
systemctl disable --now smbd nmbd
systemctl mask smbd nmbd
ss -ltn | grep ':445' && echo "port 445 is still in use" || echo "port 445 is free"
```

### 2. Create the directory and set the rootDN password

```bash
DEBIAN_FRONTEND=noninteractive dpkg-reconfigure -f noninteractive slapd
systemctl restart slapd
ldapsearch -x -H ldap://127.0.0.1 -b dc=example,dc=com -s base -LLL
```

Now set the rootDN and its password explicitly, over the local `ldapi:///`
socket with SASL EXTERNAL:

```bash
PW=$(slappasswd -s '<ldap-admin-password>' -n)
DB=$(ldapsearch -Q -Y EXTERNAL -H ldapi:/// -b cn=config -s one -LLL dn olcSuffix \
        | grep -B1 'dc=example,dc=com' \
        | awk '/^dn:/ {print $2}')
echo "configuring database $DB"

cat > /tmp/rootpw.ldif <<EOF
dn: $DB
changetype: modify
replace: olcRootDN
olcRootDN: cn=admin,dc=example,dc=com
-
replace: olcRootPW
olcRootPW: $PW
EOF

ldapmodify -Q -Y EXTERNAL -H ldapi:/// -f /tmp/rootpw.ldif
ldapwhoami -x -H ldap://127.0.0.1 \
    -D cn=admin,dc=example,dc=com -w '<ldap-admin-password>'
```

Do not rely on the preseeded password alone. slapd's configuration script
clears `slapd/password1` once it has used it, so a later
`dpkg-reconfigure slapd` recreates the database with no rootDN password at all
and every authenticated bind then fails with `Invalid credentials`. Setting it
with `ldapmodify` is repeatable.

The database DN is discovered by matching `olcSuffix` rather than hardcoded as
`olcDatabase={1}mdb,cn=config`, because its index depends on how many
databases the install created.

### 3. Load the Samba schema

```bash
ldapadd -Q -Y EXTERNAL -H ldapi:/// \
    -f /usr/share/doc/samba/examples/LDAP/samba.ldif
ldapsearch -Q -Y EXTERNAL -H ldapi:/// -b cn=schema,cn=config -LLL dn \
    | grep -i samba
```

Index the attributes samba searches on, or every logon walks the whole tree:

```bash
DB=$(ldapsearch -Q -Y EXTERNAL -H ldapi:/// -b cn=config -s one -LLL dn olcSuffix \
        | grep -B1 'dc=example,dc=com' \
        | awk '/^dn:/ {print $2}')

cat > /tmp/samba-indices.ldif <<EOF
dn: $DB
changetype: modify
add: olcDbIndex
olcDbIndex: sambaSID eq
-
add: olcDbIndex
olcDbIndex: sambaPrimaryGroupSID eq
-
add: olcDbIndex
olcDbIndex: sambaGroupType eq
-
add: olcDbIndex
olcDbIndex: sambaSIDList eq
-
add: olcDbIndex
olcDbIndex: sambaDomainName eq
EOF

ldapmodify -Q -Y EXTERNAL -H ldapi:/// -f /tmp/samba-indices.ldif
```

### 4. Point samba's passdb at the directory

```bash
cat > /etc/samba/smb.conf <<'EOF'
[global]
    workgroup = EXAMPLE
    server role = classic primary domain controller

    passdb backend = ldapsam:ldap://127.0.0.1
    ldap suffix = dc=example,dc=com
    ldap user suffix = ou=Users
    ldap group suffix = ou=Groups
    ldap machine suffix = ou=Computers
    ldap idmap suffix = ou=Idmap
    ldap admin dn = cn=admin,dc=example,dc=com
    ldap ssl = off
    ldap passwd sync = yes
    ldapsam:trusted = yes

    winbind use default domain = yes
    winbind enum users = yes
    winbind enum groups = yes
    winbind max domain connections = 5
    template shell = /bin/bash

    idmap config * : backend = tdb
    idmap config * : range = 3000-7999
EOF
testparm -s
```

Three settings in that file are load bearing, and each has a distinctive
failure if it is missing:

- **`ldapsam:trusted = yes`** lets `pdb_ldap` take `uidNumber` and `gidNumber`
  straight from the directory entry instead of making a further NSS lookup.
  Without it, `pdbedit` reports uid 4294967295 and
  `Failed to find a Unix account`.
- **`winbind max domain connections = 5`.** Whatever the passdb backend, the
  authentication stack still calls `getpwnam()` to find the POSIX identity.
  With `passwd:` pointed at winbind, that call re-enters winbindd while it is
  still handling the authentication request. With a single domain connection
  the nested lookup cannot be served, stalls for the NSS client timeout of
  about five seconds, and the logon fails with
  `User <name> in passdb, but getpwnam() fails!` followed by
  `check_sam_security: make_server_info_sam() failed, NT_STATUS_NO_SUCH_USER` -
  while `wbinfo -u`, `wbinfo -i` and `getent` all still work. Extra domain
  children let the nested lookup be answered.
- **An `nsswitch.conf` of exactly `files winbind`** (see step 7). A stray
  `nis` source on a host with no NIS domain adds seconds to every lookup,
  which widens the window above.

Note also what is deliberately *absent*: there is no
`idmap config EXAMPLE` stanza here, unlike the Active Directory procedure.
SIDs from the local SAM domain must route through `idmap_passdb` so that the
uid comes from the directory's own `uidNumber`. An explicit backend for the
workgroup would hand out uids from an idmap range instead, and files would no
longer be owned by the LDAP identity.

### 5. Give samba its bind credentials and initialise the passdb

```bash
smbpasswd -w '<ldap-admin-password>'
```

That stores the password for `ldap admin dn` in `secrets.tdb`. Now let samba
create its domain entry, and read back the SID it generated:

```bash
SID=$(net getlocalsid | awk '/^SID for domain/ {print $NF}')
echo "$SID"

ldapsearch -x -H ldap://127.0.0.1 -D cn=admin,dc=example,dc=com \
    -w '<ldap-admin-password>' -b dc=example,dc=com \
    'sambaDomainName=EXAMPLE' -LLL
```

`net getlocalsid` is what initialises the passdb: it creates the
`sambaDomainName` entry using a randomly generated domain SID when
`secrets.tdb` does not already hold one. Always read the SID back and feed it
into the tooling, rather than choosing one and pinning it in both places - if
LDAP and `secrets.tdb` disagree, the well-known groups end up under a
different domain SID than the users.

### 6. Populate the directory

Copy the configuration examples shipped with `smbldap-tools` and patch them,
so that the files always match the installed version. `smbldap-config` is
interactive and cannot be used here:

```bash
install -D -m 644 /usr/share/doc/smbldap-tools/examples/smbldap.conf \
    /etc/smbldap-tools/smbldap.conf
install -D -m 600 /usr/share/doc/smbldap-tools/examples/smbldap_bind.conf \
    /etc/smbldap-tools/smbldap_bind.conf

SID=$(net getlocalsid | awk '/^SID for domain/ {print $NF}')

sed -i -E \
    -e "s%^SID=.*%SID=\"$SID\"%" \
    -e 's%^sambaDomain=.*%sambaDomain="EXAMPLE"%' \
    -e 's%^(master|slave)LDAP=.*%\1LDAP="ldap://127.0.0.1"%' \
    -e 's%^ldapTLS=.*%ldapTLS="0"%' \
    -e 's%^suffix=.*%suffix="dc=example,dc=com"%' \
    /etc/smbldap-tools/smbldap.conf

sed -i -E \
    -e 's%^(master|slave)DN=.*%\1DN="cn=admin,dc=example,dc=com"%' \
    -e 's%^(master|slave)Pw=.*%\1Pw="<ldap-admin-password>"%' \
    /etc/smbldap-tools/smbldap_bind.conf
```

`ldapTLS="0"` is required for a plain `ldap://` connection to localhost. Keep
`smbldap_bind.conf` at mode 600: it holds the rootDN password in clear text.
If the directory is not on the same host, use `ldaps://` and leave TLS on.

Create the domain structure, then a group and a user:

```bash
printf '%s\n%s\n' '<ldap-admin-password>' '<ldap-admin-password>' \
    | smbldap-populate -g 10000 -u 10000 -r 10000
smbldap-grouplist

smbldap-groupadd -a nasusers
smbldap-useradd -a -m -g nasusers -s /bin/bash alice
printf '%s\n%s\n' '<password>' '<password>' | smbpasswd -s alice
smbpasswd -e alice
smbldap-usershow alice
```

Setting the SMB password with samba's own `smbpasswd` rather than
`smbldap-passwd` is worth doing deliberately: it also proves samba can *write*
to the directory with the bind DN it holds in `secrets.tdb`.

Confirm samba reads the account back out of LDAP, uid included:

```bash
pdbedit -L -v alice
```

### 7. Point NSS at winbind and start it

```bash
sed -i -E 's/^(passwd|group|shadow):.*/\1:files winbind/' /etc/nsswitch.conf
grep -E '^(passwd|group|shadow):' /etc/nsswitch.conf

systemctl enable --now winbind

until wbinfo -i alice >/dev/null 2>&1; do
    sleep 3
    systemctl restart winbind
    sleep 3
done

net cache flush
```

Replace those NSS lines rather than appending to them, so that no other source
survives. The `net cache flush` drops negative entries samba cached while the
user did not yet exist.

Verify authentication and, most importantly, that the uid winbind hands out is
the one in the directory:

```bash
wbinfo --own-domain
wbinfo -u | head
wbinfo -a 'alice%<password>'
ntlm_auth --username=alice --domain=EXAMPLE --password='<password>'
getent passwd alice
id alice

wbinfo --sid-to-uid="$(wbinfo -n alice | awk '{print $1}')"
ldapsearch -x -H ldap://127.0.0.1 -D cn=admin,dc=example,dc=com \
    -w '<ldap-admin-password>' -b dc=example,dc=com \
    'uid=alice' uidNumber -LLL
```

The last two commands must print the same number. If they differ, an
`idmap config` stanza is overriding `idmap_passdb` (see step 4).

### 8. Configure chimera and test

In `/usr/local/etc/chimera.json`:

```json
{
    "server": {
        "smb_enabled": true,
        "smb_auth": {
            "winbind_enabled": true,
            "winbind_domain": "EXAMPLE",
            "kerberos_enabled": false
        }
    },
    "mounts": { "data": { "module": "linux", "path": "/srv/chimera/data" } },
    "shares": { "smb1": { "path": "/data" } }
}
```

```bash
mkdir -p /srv/chimera/data
chimera -c /usr/local/etc/chimera.json

smbclient --use-kerberos=off -U 'EXAMPLE/alice%<password>' \
    //127.0.0.1/smb1 -c 'allinfo /'
smbclient --use-kerberos=off -U 'alice%<password>' \
    //127.0.0.1/smb1 -c 'allinfo /'
```

Check that ownership on disk comes from the directory, and that a bad password
is refused:

```bash
smbclient --use-kerberos=off -U 'alice%<password>' //127.0.0.1/smb1 \
    -c 'put /etc/hostname ldaptest.txt'
stat -c '%u %g %n' /srv/chimera/data/ldaptest.txt

smbclient --use-kerberos=off -U 'alice%wrongpassword' \
    //127.0.0.1/smb1 -c 'allinfo /'
```

The uid printed by `stat` must equal the `uidNumber` in LDAP. The last command
must fail with `NT_STATUS_LOGON_FAILURE` or `NT_STATUS_WRONG_PASSWORD`.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Every pass-through logon fails `NT_STATUS_LOGON_FAILURE`, but `wbinfo -a` and `ntlm_auth` succeed | Chimera started before the host was joined, so it advertises fallback names in its NTLM CHALLENGE and the domain controller's target-info check rejects the response. | Restart chimera after the join. Its log line `NTLM CHALLENGE identity: ...` shows whether the names came from `winbind` or from `fallback`. |
| `NT_STATUS_NO_SUCH_USER` after a pause of about five seconds, while `wbinfo -u`, `wbinfo -i` and `getent` all work | The nested `getpwnam()` inside the authentication request cannot be served. | Set `winbind max domain connections = 5` and reduce `nsswitch.conf` to `files winbind`. |
| `winbind_enabled: true` appears to be ignored | Chimera was built without libwbclient. | Install `libwbclient-dev` or `libwbclient-devel`, reconfigure, and look for `libwbclient found` in the cmake output. |
| Everything on disk is owned by uid 65534 | Winbind is disabled, or a Kerberos principal could not be mapped. | Set `winbind_enabled: true`, and check `wbinfo -n` and `wbinfo --sid-to-uid` for the user. |
| SMB file ownership does not match the directory's `uidNumber` | An `idmap config <workgroup>` stanza is bypassing `idmap_passdb`. | Remove it for a local SAM or ldapsam domain. |
| `adcli join` fails intermittently with `Message stream modified` | `--domain-controller` was given a name with no `ldap/` service principal, often a round-robin alias. | Use `--domain` and let adcli discover a controller. |
| The join reports it cannot store the machine password, and `wbinfo --ping-dc` then fails | On samba 4.16 and newer, `net changesecretpw` can only upgrade an existing record. | Seed the two `secrets.tdb` records with `tdbtool` before joining. |
| Kerberos works against the hostname but not `127.0.0.1` | The client derives the service principal from the name you gave it, and there is no `cifs/127.0.0.1` in the keytab. | Address the host by its real name. |
| Kerberos is attempted even though `kerberos_enabled` is `false` | SPNEGO advertises Kerberos unconditionally. | Have clients select NTLM, e.g. `smbclient --use-kerberos=off`. |
| `wbinfo -a` fails but `ntlm_auth` succeeds | `wbinfo -a` also tries an NTLMv1 challenge/response, which many domains disable. | Trust `ntlm_auth` and the `smbclient` tests; NTLMv2 is what chimera uses. |
| Chimera fails to bind port 445 | `smbd` holds it. | `systemctl disable --now smbd nmbd` and `systemctl mask smbd nmbd`. |
| `pdbedit` reports uid 4294967295 and `Failed to find a Unix account` | `ldapsam:trusted = yes` is missing. | Add it to `smb.conf` and restart winbind. |
| Authenticated LDAP binds fail `Invalid credentials` after a `dpkg-reconfigure slapd` | The preseeded rootDN password was consumed and cleared by the earlier install. | Set `olcRootPW` with `ldapmodify` over `ldapi:///`. |

When a logon fails, the two logs worth reading together are chimera's own and
winbindd's. Chimera logs the identity it advertises and the outcome of each
session setup; `-d` adds the detail of the authentication exchange:

```bash
# chimera, with debug logging enabled
chimera -d -c /usr/local/etc/chimera.json

# winbindd
journalctl -u winbind -o short-iso
```
