from pathlib import Path
p = Path('/opt/nfstest/test/nfstest_delegation')
s = p.read_text()
a = '            cbrecall_index = self.pktt.get_index()'
b = '            cbrecall_index = cbcall.record.index'
assert a in s
s = s.replace(a, b, 1)
a = '            # Find OPEN sent from the client right before returning the delegation'
assert a in s
s = s.replace(a, '            self.pktt.rewind(cbrecall_index)\n' + a, 1)
a = '                if self.pktcall:\n                    # Save lock owner'
b = '''                if lock_stid and self.pktcall.record.index < cbreply.record.index:
                    print('RECOVERED_EARLY_LOCK', self.pktcall.record.index, cbreply.record.index, delegreturn_index)
'''
assert a in s
p.write_text(s.replace(a, b + a, 1))
