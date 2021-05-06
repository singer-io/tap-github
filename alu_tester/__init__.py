from . import connections
from . import partition
from . import state
from . import user

from io import StringIO
import sys
import unittest


def capture_output(func, *args, **kwargs):
    temp_out = StringIO()
    temp_out.seek(0)

    sys.stdout = temp_out

    func(*args, **kwargs)

    sys.stdout = sys.__stdout__

    return temp_out.getvalue()


class BaseTest(unittest.TestCase):

    def setUp(self):
        # import ipdb;
        # ipdb.set_trace()
        pass

    def tearDown(self):
        pass

    def test_run(self):
        self.assertEqual({"my": "config", "bad": "config"},
                         connections.get_config())


class DiscoveryTest(BaseTest):
    pass


class AutomaticFieldsTest(BaseTest):
    pass


class StartDateTest(BaseTest):
    pass


class PaginationTest(BaseTest):
    pass


class BookmarksTest(BaseTest):
    pass


class AllFieldsTest(BaseTest):
    pass

if __name__ == '__main__':
    unittest.main()
