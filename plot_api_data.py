import pandas as pd
import matplotlib.pyplot as plt
import requests

param_name = 'sun_duration'

request = requests.get("http://localhost:8000/api/geosphere/timeseries?station_id=bozen&end_date=2026-02-15T00:00:00Z&start_date=2026-02-10T00:00:00Z&models=ensemble-v1-1h-2500m")
data = pd.DataFrame(request.json()['data'])
data['datetime'] = pd.to_datetime(data['datetime'])

plot_data = data[['datetime'] + [i for i in data.columns if i.startswith(param_name)]]
plot_data = plot_data.set_index('datetime')
plot_data.dropna(inplace = True)

fig, ax = plt.subplots()
ax.plot(plot_data.index, plot_data[f"{param_name}_p50"], color = 'black')
ax.fill_between(plot_data.index, plot_data[f"{param_name}_p10"], plot_data[f"{param_name}_p90"], alpha = .5, color = 'grey')
plt.savefig(f"{param_name}.png", dpi = 300)