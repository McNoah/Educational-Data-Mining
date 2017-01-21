# import csv

# alist, blist = [], []

# # with open("/Users/MCNOAH/Desktop/AccessLog_Tool-develop/MappedIP.csv", "r") as fileA:
# #     reader = csv.reader(fileA, delimiter='')
# #     for row in reader:
# #         # append all words in cell
# #         for word in row:
# #             alist.append(word)

# with open("/Users/MCNOAH/Desktop/AccessLog_Tool-develop/IP.csv", "r") as fileA:
#     reader = csv.reader(fileA, delimiter=',')
#     for row in reader:
#         for row_str in row:
#             alist += row_str.strip().split()

# with open("/Users/MCNOAH/Desktop/AccessLog_Tool-develop/Mapping.csv", "r") as fileB:
#     reader = csv.reader(fileB, delimiter=';')
#     for row in reader:
#         blist += row

# firstSet = set(alist)
# secondSet = set(blist)

# matches = firstSet.intersection(secondSet)
# print (matches)

# # print firstSet.intersection(secondSet)


import csv
# from collections import defaultdict
# reader1 = csv.reader(open('/Users/MCNOAH/Desktop/AccessLog_Tool-develop/MappedIP.csv', 'r'))
mylist = []
myset = set()
result = open('test.txt', 'w')
with open('IP.csv', 'r') as IPFile, open('Mapping2.csv', 'r') as IPMappedFile:
	IPs = IPFile.read().splitlines()
	IPIDs = IPMappedFile.read().splitlines()
	# for xxxx in xrange(1,len(IPs)):
	# 	print IPs[xxxx]
	# 	pass
	for x in range(1,len(IPIDs)):
		Ipx = IPIDs[x].split(',')
		# print IPs
		if Ipx[0] in IPs:
			if Ipx[0] not in myset:
				mylist.append(Ipx[0])
				myset.add(Ipx[0])
				print (Ipx[0] + ',' + Ipx[1])
				result.write(Ipx[0] + ',' + Ipx[1] + '\n')
		# else:
		# 	print Ipx[0] + "--------"
		pass


# if (row1[0] == row2[0]):
# 	print ("equal")
# else:
# 	print ("different")