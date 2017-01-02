import random
import math

# Configure paths to your dataset files here
DATASET_FILE = 'original.csv'
TrainFold01 = 'TrainFold01.csv'
TrainFold02 = 'TrainFold02.csv'
TrainFold03 = 'TrainFold03.csv'
TrainFold04 = 'TrainFold04.csv'
TrainFold05 = 'TrainFold05.csv'
TrainFold06 = 'TrainFold06.csv'
TrainFold07 = 'TrainFold07.csv'
TrainFold08 = 'TrainFold08.csv'
TrainFold09 = 'TrainFold09.csv'
TestFold = 'TestFold.csv'


# Set to true if you want to copy first line from main
# file into each split (like CSV header)
IS_CSV = True

# Make sure it adds to 100, no error checking below
PercentTrainFoldx1 = 10
PercentTrainFoldx2 = 10
PercentTrainFoldx3 = 10
PercentTrainFoldx4 = 10
PercentTrainFoldx5 = 10
PercentTrainFoldx6 = 10
PercentTrainFoldx7 = 10
PercentTrainFoldx8 = 10
PercentTrainFoldx9 = 10
PercentTestFoldx1 = 10


data = [l for l in open(DATASET_FILE, 'r')]

TrainFoldx1 = open(TrainFold01, 'w')
TrainFoldx2 = open(TrainFold02, 'w')
TrainFoldx3 = open(TrainFold03, 'w')
TrainFoldx4 = open(TrainFold04, 'w')
TrainFoldx5 = open(TrainFold05, 'w')
TrainFoldx6 = open(TrainFold06, 'w')
TrainFoldx7 = open(TrainFold07, 'w')
TrainFoldx8 = open(TrainFold08, 'w')
TrainFoldx9 = open(TrainFold09, 'w')
TestFoldx1 = open(TestFold, 'w')

if IS_CSV:
    TrainFoldx1.write(data[0])
    TrainFoldx2.write(data[0])
    TrainFoldx3.write(data[0])
    TrainFoldx4.write(data[0])
    TrainFoldx5.write(data[0])
    TrainFoldx6.write(data[0])
    TrainFoldx7.write(data[0])
    TrainFoldx8.write(data[0])
    TrainFoldx9.write(data[0])
    TestFoldx1.write(data[0])

    data = data[1:len(data)]

num_of_data = len(data)
train01 = int((PercentTrainFoldx1/100.0)*num_of_data)
train02 = int((PercentTrainFoldx2/100.0)*num_of_data)
train03 = int((PercentTrainFoldx3/100.0)*num_of_data)
train04 = int((PercentTrainFoldx4/100.0)*num_of_data)
train05 = int((PercentTrainFoldx5/100.0)*num_of_data)
train06 = int((PercentTrainFoldx6/100.0)*num_of_data)
train07 = int((PercentTrainFoldx7/100.0)*num_of_data)
train08 = int((PercentTrainFoldx8/100.0)*num_of_data)
train09 = int((PercentTrainFoldx9/100.0)*num_of_data)
test01 = int((PercentTestFoldx1/100.0)*num_of_data)


data_fractions = [train01, train02, train03, train04, train05, train06, train07, train08, train09, test01]
split_data = [[],[],[],[],[],[],[],[],[],[]]

rand_data_ind = 0

for split_ind, fraction in enumerate(data_fractions):
    for i in range(fraction):
        rand_data_ind = random.randint(0, len(data)-1)
        split_data[split_ind].append(data[rand_data_ind])
        data.pop(rand_data_ind)

for l in split_data[0]:
    TrainFoldx1.write(l)
    
for l in split_data[1]:
    TrainFoldx2.write(l)
    
for l in split_data[2]:
    TrainFoldx3.write(l)

for l in split_data[3]:
    TrainFoldx4.write(l)

for l in split_data[4]:
    TrainFoldx5.write(l)

for l in split_data[5]:
    TrainFoldx6.write(l)

for l in split_data[6]:
    TrainFoldx7.write(l)

for l in split_data[7]:
    TrainFoldx8.write(l)

for l in split_data[8]:
    TrainFoldx9.write(l)

for l in split_data[9]:
    TestFoldx1.write(l)

    
TrainFoldx1.close()
TrainFoldx2.close()
TrainFoldx3.close()
TrainFoldx4.close()
TrainFoldx5.close()
TrainFoldx6.close()
TrainFoldx7.close()
TrainFoldx8.close()
TrainFoldx9.close()
TestFoldx1.close()