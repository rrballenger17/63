
#####
# 1.

# import the necessary libraries
from pyspark.mllib.regression import LinearRegressionWithSGD 
from pyspark.mllib.tree import DecisionTree 
from pyspark.mllib.regression import LabeledPoint
import numpy as np

# set the path to the car dataset
path = "file:///home/cloudera/Desktop/Small_Car_Data_noheader.csv"

# open the data file, count the rows, and split the CSV on commas
raw_data = sc.textFile(path)
num_data = raw_data.count()
records = raw_data.map(lambda x: x.split(","))

# filter the line with NULL for horsepower
records = records.filter(lambda x: str(x[4]).replace('.', '', 1).isdigit())

# print basic info about the dataset, the first row and the number of rows
first = records.first()
print first
print num_data

# cache the dataset stored in records since it will be accessed frequently.
records.cache()

# extract features which is the displacement and return it as a dense vector
def extract_features(record):
	num_vec = np.array([float(record[3])]) 
	return num_vec

# extract the target variable which is horespower and return it as a float
def extract_label(record): 
	return float(record[4])

# map the features and target variables set using the previously declared functions
data = records.map(lambda r: LabeledPoint(extract_label(r),extract_features(r)))

# separate training and test data
data_with_idx = data.zipWithIndex().map(lambda (k, v): (v, k))
test = data_with_idx.sample(False, 0.1, 400)
train = data_with_idx.subtractByKey(test)
train_data = train.map(lambda (idx, p): p)
test_data = test.map(lambda (idx, p) : p)
train_size = train_data.count()
test_size = test_data.count()
print "Training data size: %d" % train_size
print "Test data size: %d" % test_size
print "Total data size: %d " % num_data
print "Train + Test size : %d" % (train_size + test_size)

# create linear model with the training data
linear_model = LinearRegressionWithSGD.train(train_data, iterations=10000, step=0.00001, intercept=True)
linear_model

# predict the HP values for the test dataset and compare with the actual HP values
true_vs_predicted = test_data.map(lambda p: (p.label, linear_model.predict(p.features)))


# declare functions for the MSE, MAE, and the RMSLE measurements
def squared_error(actual, pred): 
	return (pred - actual)**2

def squared_log_error(pred, actual):
	return (np.log(pred + 1) - np.log(actual + 1))**2

def abs_error(actual, pred):
	return np.abs(pred - actual)

# print the errors on the test data predictions
mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean() 
rmsle=np.sqrt(true_vs_predicted.map(lambda(t,p):squared_log_error(t,p)).mean()) 
print "Linear Model predictions: " + str(true_vs_predicted.take(5))
print "Linear Model: " + str(linear_model)
print "Linear Model - Mean Squared Error: %2.4f" % mse
print "Linear Model - Mean Absolute Error: %2.4f" % mae
print "Linear Model - Root Mean Squared Log Error: %2.4f" % rmsle

# original data RDD
#test_data.saveAsTextFile("file:///home/cloudera/Desktop/original.csv")


# predictions RDD
#predictions = test_data.map(lambda p: (linear_model.predict(p.features), p.features[0]))
#predictions.saveAsTextFile("file:///home/cloudera/Desktop/predictions.csv")











