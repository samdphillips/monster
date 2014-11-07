
from collections import namedtuple


DEFAULT_NUM_TUPLES = 32


_tid = namedtuple('_tid', 'addr chunk offset')


class NoSuchTuple(Exception):
    pass


_default = object()

class TSpace(object):
    def __init__(self):
        self._indexes = []
        self._chunks  = []
        self._count   = 0
        self._free    = None

    def _make_tid(self, addr):
        chunk  = addr / DEFAULT_NUM_TUPLES
        offset = addr % DEFAULT_NUM_TUPLES
        return _tid(addr, chunk, offset)

    def _find_free_space(self):
        if self._free is None:
            self._allocate_tuples()
        tid = self._free
        self._free = self._get(tid)
        return tid

    def _allocate_tuples(self):
        new = [self._make_tid(self._count + x + 1) for x in xrange(DEFAULT_NUM_TUPLES)]
        new[-1] = None
        self._free = self._make_tid(self._count)
        self._chunks.append(new)

    def _add_tuple(self, tid, obj):
        c = self._chunks[tid.chunk]
        c[tid.offset] = obj

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
        try:
            c = self._chunks[tid.chunk]
            return c[tid.offset]
        except IndexError:
            raise NoSuchTuple

    def get(self, tid, default=_default):
        try:
            v = self._get(tid)
            if isinstance(v, _tid):
                raise NoSuchTuple
        except NoSuchTuple:
            if default is not _default:
                return default
            raise
        return v

    def remove(self, tid):
        c = self._chunks[tid.chunk]
        c[tid.offset] = self._free
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
        self.assertIsInstance(tid, _tid)
        self.assertNotEqual(tid, None)

    def test_put2(self):
        tid1 = self.tspace.put(self.v)
        tid2 = self.tspace.put(self.v)
        self.assertIsInstance(tid2, _tid)
        self.assertNotEqual(tid2, None)
        self.assertNotEqual(tid1, tid2)

    def test_put_chunk(self):
        tids = []
        for x in xrange(DEFAULT_NUM_TUPLES + 1):
            tids.append(self.tspace.put({'a': x}))

        for tid,x in zip(tids, xrange(DEFAULT_NUM_TUPLES + 1)):
            self.assertEqual(self.tspace.get(tid)['a'], x,
                    'value incorrect for tid: %s' % `tid`)

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

    def test_get_missing(self):
        tid = self.tspace._make_tid(0)
        with self.assertRaises(NoSuchTuple):
            self.tspace.get(tid)

    def test_get_default(self):
        tid = self.tspace._make_tid(0)
        v = self.tspace.get(tid, 42)
        self.assertEqual(v, 42)

    def test_get_default_none(self):
        tid = self.tspace._make_tid(0)
        v = self.tspace.get(tid, None)
        self.assertEqual(v, None)

    def test_get_missing_outside_chunk(self):
        self.tspace._allocate_tuples()
        tid = self.tspace._make_tid(DEFAULT_NUM_TUPLES + 1)
        with self.assertRaises(NoSuchTuple):
            self.tspace.get(tid)

    def test_get_missing_inside_chunk(self):
        self.tspace._allocate_tuples()
        tid = self.tspace._make_tid(1)
        with self.assertRaises(NoSuchTuple):
            self.tspace.get(tid)


if __name__ == '__main__':
    unittest.main()
