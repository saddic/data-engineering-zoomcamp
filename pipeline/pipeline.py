import sys
import pandas as pd

print('Arguements passed to the script:', sys.argv)
month = int(sys.argv[1])
df = pd.DataFrame({'day':[1,2], 'number_of_passengers':[100, 150], 'month':[month, month]})
print(df.head())
df.to_parquet(f'output_{month}.parquet', index=False)
print(f"Pipeline module loaded successfully. Month: {month}")