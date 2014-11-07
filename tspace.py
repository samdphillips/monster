
DEFAULT_NUM_TUPLES = 32

class TSpace(object):
    def __init__(self):
        self._indexes = []
        self._chunks  = []
        self._count   =  0
        self._free    = -1

    def _find_free_space(self):
        if self._free == -1:
            return None
        return self._free

    def _allocate_tuples(self):
        new = [self._count + x + 1 for x in xrange(DEFAULT_NUM_TUPLES)]
        new[-1] = -1
        self._free = self._count + 1
        tid = self._count
        self._chunks.append(new)
        return tid

    def _add_tuple(self, tid, obj):
        c = tid / DEFAULT_NUM_TUPLES
        o = tid % DEFAULT_NUM_TUPLES
        self._chunks[c][o] = obj

    def _update_index(self, tid, o):
        pass

    def add_index(self, index):
        pass

    def put(self, obj):
        tid = self._find_free_space()
        if tid is None:
            tid = self._allocate_tuples()
        self._add_tuple(tid, obj)
        self._update_index(tid, obj)
        return tid

    def get(self, tid):
        c = tid / DEFAULT_NUM_TUPLES
        o = tid % DEFAULT_NUM_TUPLES
        return self._chunks[c][o]

    def remove(self, tid):
        pass

    def find(self, query):
        pass

    def subscribe(self, query):
        pass

    def unsubscribe(self, subid):
        pass

import unittest

class TSpaceTests(unittest.TestCase):
    def setUp(self):
        self.tspace = TSpace()
        self.v = {'a': 42}

    def test_put(self):
        tid = self.tspace.put(self.v)
        self.assertIsInstance(tid, int)
        self.assertNotEqual(tid, -1)

    def test_put2(self):
        tid1 = self.tspace.put(self.v)
        tid2 = self.tspace.put(self.v)
        self.assertIsInstance(tid2, int)
        self.assertNotEqual(tid2, -1)
        self.assertNotEqual(tid1, tid2)

    def test_get(self):
        tid = self.tspace.put(self.v)
        self.assertEqual(self.tspace.get(tid), self.v)

if __name__ == '__main__':
    unittest.main()
