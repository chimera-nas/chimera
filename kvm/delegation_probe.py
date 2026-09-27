from pathlib import Path
p = Path('/opt/nfstest/test/nfstest_delegation')
s = p.read_text()
needle = '                if self.pktcall:\n                    # Save lock owner'
probe = '''                if lock_stid is None:
                    print('DELEG_DIAG bounds', open1_index, cbcall.record.index, cbreply.record.index if cbreply else None, open_index, delegreturn_index)
                    for pkt in self.pktt.pktlist:
                        if open1_index <= pkt.record.index <= delegreturn_index:
                            ops = [getattr(op, 'argop', getattr(op, 'resop', None)) for op in pkt.nfs.array]
                            if OP_LOCK in ops:
                                print('DELEG_DIAG lock', pkt.record.index, pkt.rpc.xid, pkt.rpc.type, ops)
'''
assert needle in s
p.write_text(s.replace(needle, probe + needle, 1))
