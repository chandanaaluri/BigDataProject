import pymongo, pandas as pd, plotly.express as px

client = pymongo.MongoClient("mongodb://localhost:27017")
df = pd.DataFrame(list(client["agri_db"]["sensor_readings"].find()))
if df.empty:
    print("No data.")
    exit()

df['ts'] = pd.to_datetime(df['ts'])
fig = px.line(df.sort_values('ts'), x='ts', y='soil_moisture', color='sensor_id')
fig.show()
