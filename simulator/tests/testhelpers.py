class TestHelpers(object):
    def assertExtends(self, a, b):
        """
        Test whether dict a extends dict b.
        """
        for k, v in b.items():
            self.assertEqual(a[k], v)
