# parallel-isin
Faster isin() by employing multiprocessing.

# Example
```python
import pandas as pd
import numpy as np

# make data
data = pd.DataFrame(np.arange(1e8))
condition = list(np.arange(1e3))

# isin
outcome_df = data.isin(condition)
```
The above code get the same outcome with below one, but below one is faster.
```python
import pandas as pd
import numpy as np
from multiple_processes import parallel_isin

# make data
data = pd.DataFrame(np.arange(1e8))
condition = list(np.arange(1e3))

# isin
outcome_pl = parallel_isin(
    data,    # 原始資料
    condition,    # isin 的內容
    slice_num=1000,    # 要將原始資料切成幾份下去平行跑
    num_processes=6)    # 要使用幾個核心來跑
```
