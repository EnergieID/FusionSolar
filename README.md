# FusionSolar
Python client for Huawey FusionSolar API

## Example usage

```python
from fusionsolar import Client, PandasClient
import pandas as pd

date = pd.Timestamp('20200402', tz='Europe/Brussels')

with PandasClient(user_name=user, system_code=password) as client:
    sl = client.get_station_list()
    station_code = sl['data'][0]['stationCode']
    
    df = client.get_kpi_day(station_code=station_code, date=date)

print(df)
```