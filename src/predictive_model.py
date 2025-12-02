import numpy as np, pandas as pd, pymongo, joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report

client = pymongo.MongoClient("mongodb://localhost:27017")
df = pd.DataFrame(list(client["agri_db"]["sensor_readings"].find()))

if df.empty:
    n=2000
    df = pd.DataFrame({
        'soil_moisture': np.random.normal(40,12,n).clip(0,100),
        'temperature': np.random.normal(25,4,n),
        'humidity': np.random.uniform(30,90,n),
        'rainfall': np.abs(np.random.normal(0.2,0.6,n))
    })
    df['irrigation_needed'] = ((df.soil_moisture<30)&(df.rainfall<0.5)).astype(int)
else:
    df['irrigation_needed'] = df['irrigation_needed'].astype(int)

X = df[['soil_moisture','temperature','humidity','rainfall']]
y = df['irrigation_needed']
X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=0.2,random_state=42)
clf = RandomForestClassifier().fit(X_train,y_train)
print(classification_report(y_test, clf.predict(X_test)))
joblib.dump(clf, "/mnt/data/agri_project/irrigation_model.joblib")
