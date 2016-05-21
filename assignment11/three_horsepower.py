from pyspark.mllib.regression import LinearRegressionWithSGD 
from pyspark.mllib.tree import DecisionTree 
from pyspark.mllib.regression import LabeledPoint
import numpy as np

path = "file:///home/cloudera/Desktop/Small_Car_Data_noheader.csv"

# import and extract the data from the csv
raw_data = sc.textFile(path)
num_data = raw_data.count()
records = raw_data.map(lambda x: x.split(","))
# filter out line with nan horsepower
records = records.filter(lambda x: str(x[4]).replace('.', '', 1).isdigit())
first = records.first()
print first
print num_data

records.cache()

# map function for the categorical variables
def get_mapping(rdd, idx):
	return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()

# mappings for categorical variables
mappings = [get_mapping(records, i) for i in [5,9]] 

# concatenate numeric and categorical features into floats
def extract_features_dt(record):
	num_vec = [float(record[2]), float(record[3]), float(record[7]), float(record[10]), float(mappings[0][record[5]]),   float(mappings[1][record[9]])] 
	return map(float, num_vec)

def extract_label_acc(record): 
	return float(record[1])

def extract_label_hp(record): 
	return float(record[4])

# horsepower model
data_dt = records.map(lambda r: LabeledPoint(extract_label_hp(r), extract_features_dt(r)))

# get 90% train and 10% test data
data_with_idx = data_dt.zipWithIndex().map(lambda (k, v): (v, k))
test = data_with_idx.sample(False, 0.1)
train = data_with_idx.subtractByKey(test)
train_data = train.map(lambda (idx, p): p)
test_data = test.map(lambda (idx, p) : p)
train_size = train_data.count()
test_size = test_data.count()
print "Training data size: %d" % train_size
print "Test data size: %d" % test_size
print "Total data size: %d " % num_data
print "Train + Test size : %d" % (train_size + test_size)

# make decision tree model 
dt_model = DecisionTree.trainRegressor(train_data,{})

# make predictions and measure error
preds = dt_model.predict(test_data.map(lambda p: p.features))
actual = test_data.map(lambda p: p.label)
true_vs_predicted_dt = actual.zip(preds)
print "Decision Tree predictions: " + str(true_vs_predicted_dt.take(5))
print "Decision Tree depth: " + str(dt_model.depth())
print "Decision Tree number of nodes: " + str(dt_model.numNodes())

def squared_error(actual, pred): 
	return (pred - actual)**2

def squared_log_error(pred, actual):
	return (np.log(pred + 1) - np.log(actual + 1))**2

def abs_error(actual, pred):
	return np.abs(pred - actual)

mse = true_vs_predicted_dt.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted_dt.map(lambda (t, p): abs_error(t, p)).mean() 
rmsle=np.sqrt(true_vs_predicted_dt.map(lambda(t,p):squared_log_error(t,p)).mean()) 
print "Decision Tree predictions: " + str(true_vs_predicted_dt.take(5))
print "Decision Tree - Mean Squared Error: %2.4f" % mse
print "Decision Tree - Mean Absolute Error: %2.4f" % mae
print "Decision Tree - Root Mean Squared Log Error: %2.4f" % rmsle

