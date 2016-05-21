from pyspark.mllib.regression import LinearRegressionWithSGD 
from pyspark.mllib.tree import DecisionTree 
from pyspark.mllib.regression import LabeledPoint
import numpy as np

path = "file:///home/cloudera/Desktop/Small_Car_Data_noheader.csv"

# load and extract data from the csv
raw_data = sc.textFile(path)
num_data = raw_data.count()
records = raw_data.map(lambda x: x.split(","))

# filter out line with nan horsepower
records = records.filter(lambda x: str(x[4]).replace('.', '', 1).isdigit())

first = records.first()
print first
print num_data

# cache the records since it's used frequently
records.cache()

# map function for categorical variables
def get_mapping(rdd, idx):
	return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()

mappings = [get_mapping(records, i) for i in [5,9]] 
cat_len = sum(map(len, mappings))
num_len = 4
total_len = num_len + cat_len

# map categorical variables and concatenate with numeric feature variables
def extract_features(record):
	cat_vec = np.zeros(cat_len) 
	i =0
	step = 0
	for field in [record[5],record[9]]:
		m = mappings[i]
		idx = m[field] 
		cat_vec[idx + step] = 1 
		i= i + 1
		step = step + len(m)
	num_vec = np.array([float(record[2]), float(record[3]), float(record[7]), float(record[10])]) 
	return np.concatenate((cat_vec, num_vec))

def extract_label_acc(record): 
	return float(record[1])

def extract_label_hp(record): 
	return float(record[4])

# horsepower model

data = records.map(lambda r: LabeledPoint(extract_label_hp(r),extract_features(r)))

# separate 10% test and 90% training data

data_with_idx = data.zipWithIndex().map(lambda (k, v): (v, k))
test = data_with_idx.sample(False, 0.1, 100)
train = data_with_idx.subtractByKey(test)
train_data = train.map(lambda (idx, p): p)
test_data = test.map(lambda (idx, p) : p)
train_size = train_data.count()
test_size = test_data.count()
print "Training data size: %d" % train_size
print "Test data size: %d" % test_size
print "Total data size: %d " % num_data
print "Train + Test size : %d" % (train_size + test_size)

# make the linear regression horsepower model

linear_model_hp = LinearRegressionWithSGD.train(train_data, iterations=100, step=0.0000001, intercept=False)
linear_model_hp

# make predictions and measure error
true_vs_predicted = test_data.map(lambda p: (p.label, linear_model_hp.predict(p.features)))

def squared_error(actual, pred): 
	return (pred - actual)**2

def squared_log_error(pred, actual):
	return (np.log(pred + 1) - np.log(actual + 1))**2

def abs_error(actual, pred):
	return np.abs(pred - actual)

mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean() 
rmsle=np.sqrt(true_vs_predicted.map(lambda(t,p):squared_log_error(t,p)).mean()) 
print "Linear Model predictions: " + str(true_vs_predicted.take(5))
print "Linear Model - Mean Squared Error: %2.4f" % mse
print "Linear Model - Mean Absolute Error: %2.4f" % mae
print "Linear Model - Root Mean Squared Log Error: %2.4f" % rmsle



