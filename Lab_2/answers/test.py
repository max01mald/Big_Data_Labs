import sklearn
import numpy as np
from sklearn import datasets
iris = datasets.load_iris()

print(iris.feature_names)
print(iris.data)
print(iris.target)
print(iris.target_names)

from sklearn.model_selection import train_test_split

X = iris.data
Y = iris.target

X_train, X_test, Y_train, Y_test = train_test_split(X,Y,test_size=0.4)

from sklearn.ensemble import RandomForestClassifier

rf = RandomForestClassifier(n_estimators=10, random_state=123456)
rf = rf.fit(X_train, Y_train)

from sklearn.metrics import accuracy_score

predicted = rf.predict(X_test)
accuracy = accuracy_score(Y_test, predicted)

print(f'Mean accuracy score: {accuracy:.3}')
