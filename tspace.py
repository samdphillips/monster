
from collections import namedtuple
from itertools import chain, count, ifilter, imap, izip
from operator import methodcaller


DEFAULT_CHUNK_SIZE = 32


_tid = namedtuple('_tid', 'addr chunk offset')


class NoSuchTuple(Exception):
    pass


class BadPut(Exception):
    pass


_default = object()

class TSpace(object):
    def __init__(self):
        self._indexes = []
        self._chunks  = []
        self._count   = 0
        self._free    = None

    def _make_tid(self, addr):
        chunk  = addr / DEFAULT_CHUNK_SIZE
        offset = addr % DEFAULT_CHUNK_SIZE
        return _tid(addr, chunk, offset)

    def _find_free_space(self):
        if self._free is None:
            self._allocate_tuples()
        tid = self._free
        self._free = self._get(tid)
        return tid

    def _allocate_tuples(self):
        new = [self._make_tid(self._count + x + 1) for x in xrange(DEFAULT_CHUNK_SIZE)]
        new[-1] = None
        self._free = self._make_tid(self._count)
        self._chunks.append(new)

    def _add_tuple(self, tid, obj, check=True):
        c = self._chunks[tid.chunk]
        if check and c[tid.offset] != self._free:
            raise BadPut
        c[tid.offset] = obj

    def _update_index(self, tid, o, remove=False):
        if remove:
            f = methodcaller('remove_tuple', tid, o)
        else:
            f = methodcaller('add_tuple', tid, o)

        for i in self._indexes:
            f(i)

    def add_index(self, index):
        for tid, v in self.all_tuples():
            index.add_tuple(tid, v)
        self._indexes.append(index)

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
            raise NoSuchTuple(tid)

    def get(self, tid, default=_default):
        try:
            v = self._get(tid)
            if isinstance(v, _tid):
                raise NoSuchTuple(tid)
        except NoSuchTuple:
            if default is not _default:
                return default
            raise
        return v

    def remove(self, tid):
        try:
            obj = self._get(tid)
        except NoSuchTuple:
            return

        if isinstance(obj, _tid):
            return
        self._add_tuple(tid, self._free, check=False)
        self._free = tid
        self._count -= 1
        self._update_index(tid, obj, remove=True)

    def free_list(self):
        free = set()
        curr = self._free
        while curr is not None:
            free.add(curr)
            curr = self._get(curr)
        return free

    def all_tids(self):
        free = self.free_list()
        n = len(self._chunks) * DEFAULT_CHUNK_SIZE
        return ifilter(lambda x: x not in free, imap(self._make_tid, xrange(n)))

    def all_tuples(self):
        for tid in self.all_tids():
            yield tid, self.get(tid)

    def find(self, query):
        pass

    def subscribe(self, query):
        pass

    def unsubscribe(self, subid):
        pass

import random
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
        for x in xrange(DEFAULT_CHUNK_SIZE + 1):
            tids.append(self.tspace.put({'a': x}))

        for tid,x in zip(tids, xrange(DEFAULT_CHUNK_SIZE + 1)):
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

    def test_remove_missing_allocated(self):
        self.tspace._allocate_tuples()
        tid = self.tspace._make_tid(1)
        free = self.tspace._free
        self.tspace.remove(tid)
        self.assertEqual(self.tspace._free, free)

    def test_remove_missing_allocated(self):
        tid = self.tspace._make_tid(1)
        free = self.tspace._free
        self.tspace.remove(tid)
        self.assertEqual(self.tspace._free, free)

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
        tid = self.tspace._make_tid(DEFAULT_CHUNK_SIZE + 1)
        with self.assertRaises(NoSuchTuple):
            self.tspace.get(tid)

    def test_get_missing_inside_chunk(self):
        self.tspace._allocate_tuples()
        tid = self.tspace._make_tid(1)
        with self.assertRaises(NoSuchTuple):
            self.tspace.get(tid)

    def test_put_out_of_range(self):
        tid = self.tspace._make_tid(0)
        with self.assertRaises(IndexError):
            self.tspace._add_tuple(tid, self.v)

    def test_put_free_interior(self):
        self.tspace._allocate_tuples()
        tid = self.tspace._make_tid(1)
        with self.assertRaises(BadPut):
            self.tspace._add_tuple(tid, self.v)

    def test_stress_random(self):
        r0 = random.Random(1277)
        for x in xrange(10):
            seed = r0.getrandbits(32)
            self.rand_test(seed)

    def rand_test(self, seed):
        r = random.Random(seed)
        tuples = []
        for y in xrange(1000):
            a = r.choice(['get', 'put', 'remove'])
            if a == 'put':
                v = {'a': r.random()}
                tid = self.tspace.put(v)
                tuples.append((tid, v))
            elif len(tuples) != 0  and a == 'get':
                tid, v = r.choice(tuples)
                self.assertEqual(self.tspace.get(tid), v)
            elif len(tuples) != 0 and a == 'remove':
                i = r.randrange(0, len(tuples)) 
                tid, v = tuples[i]
                del tuples[i]
                self.tspace.remove(tid)


import mock

from mock import call

class TSpaceIndexTests(unittest.TestCase):
    def setUp(self):
        self.tspace = TSpace()
        self.index  = mock.Mock()

    def test_add_index_empty(self):
        self.tspace.add_index(self.index)
        self.assertEqual(self.index.mock_calls, [])
        self.assertEqual(self.tspace._indexes[0], self.index)

    def test_add_index_contents(self):
        v = {'a': 42}
        tid1 = self.tspace.put(v)
        tid2 = self.tspace.put(v)
        tid3 = self.tspace.put(v)
        self.tspace.add_index(self.index)
        calls = [ call.add_tuple(t, v) for t in [tid1, tid2, tid3]]
        self.index.assert_has_calls(calls)

    def test_update_index_add(self):
        v = {'a': 42}
        self.tspace.add_index(self.index)
        tid = self.tspace.put(v)
        self.index.assert_has_calls([call.add_tuple(tid, v)])

    def test_update_index2_add(self):
        v = {'a': 42}
        index2 = mock.Mock()
        self.tspace.add_index(self.index)
        self.tspace.add_index(index2)
        tid = self.tspace.put(v)
        self.index.assert_has_calls([call.add_tuple(tid, v)])
        index2.assert_has_calls([call.add_tuple(tid, v)])

    def test_update_index_remove(self):
        v = {'a': 42}
        self.tspace.add_index(self.index)
        tid = self.tspace.put(v)
        self.tspace.remove(tid)
        self.index.assert_has_calls([call.add_tuple(tid, v), call.remove_tuple(tid, v)])

    def test_update_index2_remove(self):
        v = {'a': 42}
        index2 = mock.Mock()
        self.tspace.add_index(self.index)
        self.tspace.add_index(index2)
        tid = self.tspace.put(v)
        self.tspace.remove(tid)
        self.index.assert_has_calls([call.add_tuple(tid, v), call.remove_tuple(tid, v)])
        index2.assert_has_calls([call.add_tuple(tid, v), call.remove_tuple(tid, v)])

if __name__ == '__main__':
    unittest.main()
