"""
Computes the rank for an arbitrary number of items given a win matrix. The win matrix is defined as a matrix W where
each entry W_i,j = N means that item i was chosen over item j a total of N times. This uses the RankCentrality method
as described by Negahban, Oh and Shah, 2014.
"""


import logging
import numpy as np
from scipy import sparse
from scipy.sparse.linalg import gmres, spsolve

_log = logging.getLogger(__name__)


def _comp_dmax(W):
    """
    Computes the reciprocal of the maximal out degree of the win matrix W.

    :param W: An N x N win matrix, as in rank()
    :return: A float, 1/d_max
    """
    row_counts = np.diff(W.tocsr().indptr)
    d_max = np.max(row_counts)
    return 1 / float(d_max)


def _w_to_p(W):
    """
    Computes the time-independent transition matrix P
    :param W: An N x N win matrix, as in rank()
    :return: The time-independent transition matrix P
    """
    # ensure that the win matrix is LIL-type sparse matrix. Also transpose it, effectively converting it to a 'loss'
    # matrix.
    W = sparse.lil_matrix(W.T, dtype=np.int)
    rdmax = _comp_dmax(W)
    a, b = W.nonzero()  # this is an inefficient method, but I can't think of a better way to do it.
    P = W.astype(float)
    P[a, b] = W[a, b] / (W[a, b] + W[b, a])
    P = P.tocsr()
    P = P.multiply(rdmax)  # normalize
    dr, dc = np.diag_indices_from(P)  # obtain the diagonal indices.
    P[dr, dc] = 1 - np.squeeze(P.sum(1).A)  # constrain the rows to sum to precisely 1.
    P.eliminate_zeros()
    return P


def _markov_stationary_components(P, mean=1.0, tol=1e-12):
    """
    Splits the transition matrix P into connected components and finds the stationary state for each.

    :param P: The N x N transition matrix, as obtained from the win matrix computed by _w_to_p()
    :param mean: The mean value of the rankings, as in rank()
    :param tol: The tolerance, as in rank()
    :return: A length-N numpy array of floats corresponding to the ranks of each item
    """
    n = P.shape[0]
    n_components, labels = sparse.csgraph.connected_components(P, directed=True, connection='strong')
    sizes = [(labels == j).sum() for j in range(n_components)]
    _log.debug('%i components, larges is %i'%(max(labels) + 1, max(sizes)))
    p = np.zeros(n)
    for comp in range(n_components):
        indices = np.flatnonzero(labels == comp)
        subP = P[indices, :][:, indices]
        p[indices] = _markov_stationary_component(subP, mean, tol)
    return p


def _markov_stationary_component(subP, mean=1.0, tol=1e-12, direct=False):
    """
    Returns the stationary state of the Markov chain for a single connected component P called subP.

    :param subP: An M x M single connected component of P
    :param mean: The mean value of the rankings, as in rank()
    :param tol: The tolerance of the estimate, as in rank()
    :param direct: If True, uses a direct method (spsolve), else it uses an iterative method.
    :return: A length-M numpy array of floats corresponding to the rankings for this connected component.
    """
    if 1 >= subP.size:
        # account for when the the transition matrix is inappropriately sized.
        return np.array([1.0]*subP.size)
    n = subP.shape[0]
    dP = subP - sparse.eye(n)
    A = sparse.vstack([np.ones(n), dP.T[1:, :]])
    rhs = np.zeros((n,))
    rhs[0] = n * mean
    if direct:
        return spsolve(A, rhs)
    else:
        # use GMRES
        p, info = gmres(A, rhs, tol=tol)
    if info:
        _log.warning("GMRES did not converge!")
    return p


def rank(W, mean=1.0, tol=1e-12):
    """
    Computes the rank for an arbitrary number of items given a win matrix. The win matrix is defined as a matrix W where
    each entry W_i,j = N means that item i was chosen over item j a total of N times. This uses the RankCentrality
    method as described by Negahban, Oh and Shah, 2014.

    :param W: An N x N matrix whose entries i,j are integers, indicating the number of times i has been chosen over j.
    :param mean: The mean value of the rankings, such that the average score is equal to mean.
    :param tol: The epsilon tolerance, used during iterative estimation of the ranks.
    :return: A length-N numpy array of floats corresponding to the ranks of each item.
    """
    if 1 >= W.size:
        return np.array([1.0]*W.size)
    P = _w_to_p(W)
    return _markov_stationary_components(P, mean=mean, tol=tol)

