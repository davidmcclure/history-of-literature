

import numpy as np

from collections import OrderedDict
from sklearn.neighbors import KernelDensity

from hol import config
from hol.models import AnchoredCount


@config.mem.cache
def mdw(*args, **kwargs):

    """
    Cache AnchoredCount.mdw().

    Returns: dict
    """

    return AnchoredCount.mdw(*args, **kwargs)


@config.mem.cache
def pdf(series, years, bandwidth=5):

    """
    Given a rank series and year range, estimate a PDF.

    Args:
        series (OrderedDict<year, rank>)
        years (iter)
        bandwidth (int)

    Returns: dict
    """

    data = []
    for year, rank in series.items():
        data += [year] * rank

    data = np.array(data)[:, np.newaxis]

    pdf = KernelDensity(bandwidth=bandwidth).fit(data)

    samples = OrderedDict()

    for year in years:
        samples[year] = np.exp(pdf.score(year))

    return samples
