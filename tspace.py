
DEFAULT_NUM_TUPLES = 32

class NoSuchTuple(Exception):
    pass

class TSpace(object):
    def __init__(self):
        self._indexes = []
        self._chunks  = []
        self._count   =  0
        self._free    = -1

    def _find_free_space(self):
        if self._free == -1:
            self._allocate_tuples()
        tid = self._free
        self._free = self._get(tid)
        return tid

    def _allocate_tuples(self):
        new = [self._count + x + 1 for x in xrange(DEFAULT_NUM_TUPLES)]
        new[-1] = -1
        self._free = self._count
        self._chunks.append(new)

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
        self._add_tuple(tid, obj)
        self._update_index(tid, obj)
        self._count += 1
        return tid

    def _get(self, tid):
        c = tid / DEFAULT_NUM_TUPLES
        o = tid % DEFAULT_NUM_TUPLES
        return self._chunks[c][o]

    def get(self, tid):
        v = self._get(tid)
        if isinstance(v, int):
            raise NoSuchTuple
        return v

    def remove(self, tid):
        c = tid / DEFAULT_NUM_TUPLES
        o = tid % DEFAULT_NUM_TUPLES
        self._chunks[c][o] = self._free
        self._free = tid

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

    def test_put_chunk(self):
        tids = []
        for x in xrange(DEFAULT_NUM_TUPLES + 1):
            tids.append(self.tspace.put({'a': x}))

        for tid,x in zip(tids, xrange(DEFAULT_NUM_TUPLES + 1)):
            self.assertEqual(self.tspace.get(tid)['a'], x,
                    'value incorrect for tid: %s' % tid)

    def test_get(self):
        tid = self.tspace.put(self.v)
        self.assertEqual(self.tspace.get(tid), self.v)

    def test_remove(self):
        tid = self.tspace.put(self.v)
        self.tspace.remove(tid)
        # XXX: checks an implementation detail
        self.assertEqual(self.tspace._free, tid)

    def test_remove_get(self):
        tid = self.tspace.put(self.v)
        self.tspace.remove(tid)
        with self.assertRaises(NoSuchTuple):
            self.tspace.get(tid)


if __name__ == '__main__':
    unittest.main()
