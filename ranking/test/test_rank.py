import unittest
from ranking.rank_from_wm import rank
import numpy as np


class TestRank(unittest.TestCase):

    def setUp(self):
        self.eps = 1e-5
        self.n = 5
        self.tot_games = 1e7

    def test_empty_matrix(self):
        WM = np.array([])
        r = rank(WM)
        self.assertTrue(np.array_equiv(r, np.array([])))

    def test_size_one_matrix(self):
        WM = np.array([1])
        r = rank(WM)
        self.assertEqual(r, np.array([1.0]))

    def test_rank_centrality(self):
        p = np.random.rand(self.n) + self.eps
        p /= np.mean(p)
        WM = np.zeros((self.n, self.n))
        for n, i in enumerate(p):
            for m, j in enumerate(p):
                # let's just set the values
                if m <= n:
                    continue
                wins_n = int(self.tot_games * (i / (i + j)))
                wins_m = int(self.tot_games * (j / (i + j)))
                WM[n, m] = wins_n
                WM[m, n] = wins_m
        r = rank(WM)
        diff = np.abs(r - p)
        for i in diff:
            self.assertAlmostEquals(i, 0.0, delta=self.eps)

if __name__ == '__main__':
    unittest.main()
