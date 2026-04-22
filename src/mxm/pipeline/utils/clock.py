from __future__ import annotations

import numpy as np

from mxm.types.timestamps import TSNSScalar


def utc_now_ts_ns() -> TSNSScalar:
    """
    Return current time as canonical MXM timestamp (np.datetime64[ns], UTC).

    This is a runtime boundary function:
    - not deterministic
    - not suitable for kernel logic
    - safe for orchestration / reporting layers
    """
    # numpy now is already UTC-based under POSIX semantics
    return np.datetime64("now", "ns")
