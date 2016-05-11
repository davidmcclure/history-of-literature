

from hol import config
from hol.models import AnchoredCount


@config.mem.cache
def mdw(*args, **kwargs):

    """
    Cache AnchoredCount.mdw().
    """

    return AnchoredCount.mdw(*args, **kwargs)
