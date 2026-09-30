# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense

# Python 3.14 compatibility for the external /opt/pynfs checkout, loaded by
# putting this directory first on PYTHONPATH (see scripts/pynfs_*_wrapper.sh).

try:
    from xdrlib3 import Packer
except ImportError:
    Packer = None

if Packer is not None:
    _pack_string = Packer.pack_string

    def _chimera_pack_string(self, s):
        if isinstance(s, str):
            s = s.encode()
        return _pack_string(self, s)

    Packer.pack_string = _chimera_pack_string

# pynfs's nfs4lib.SSVContext (used by the SP4_SSV exchange_id test, EID50) is
# still Python-2 era: the initial SSV is the str "'\0' * ssv_len" and str_xor
# builds a str via chr(ord(...)), so hmac.new() and the XOR both blow up on
# Python 3 (hmac keys must be bytes; iterating bytes yields ints).  Coerce both
# to bytes.  nfs4lib is not importable at interpreter startup (the testserver
# adds its own dir to sys.path only once it runs), and the 4.0/4.1 trees each
# ship their own copy, so patch via a post-import hook keyed on the module name
# rather than importing it here -- that way we patch whichever nfs4lib the
# running testserver actually loaded, without perturbing sys.path.
import sys as _sys
import builtins as _builtins

_ssv_patched = [False]

def _chimera_patch_nfs4lib(m):
    def _str_xor(a, b):
        if isinstance(a, str):
            a = a.encode("latin-1")
        if isinstance(b, str):
            b = b.encode("latin-1")
        return bytes(x ^ y for x, y in zip(a, b))
    m.str_xor = _str_xor

    ssv = getattr(m, "SSVContext", None)
    if ssv is not None:
        _orig_subkey = ssv._subkey

        def _subkey(self, val, i):
            if isinstance(val, str):
                val = val.encode("latin-1")
            return _orig_subkey(self, val, i)
        ssv._subkey = _subkey

_real_import = _builtins.__import__

def _chimera_import(name, *args, **kwargs):
    mod = _real_import(name, *args, **kwargs)
    if not _ssv_patched[0]:
        m = _sys.modules.get("nfs4lib")
        if m is not None and hasattr(m, "SSVContext"):
            _chimera_patch_nfs4lib(m)
            _ssv_patched[0] = True
    return mod

_builtins.__import__ = _chimera_import
