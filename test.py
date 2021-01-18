import tushare as ts
df = ts.get_hist_data("002002", '2021-01-01', '2021-01-18')
print(len(df))
