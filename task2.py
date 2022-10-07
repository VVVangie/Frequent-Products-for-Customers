import pyspark
import sys
import csv
import time
import math
import itertools
import collections

#/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task2.py

#input_file_path='../resource/asnlib/publicdata/ta_feng_all_months_merged.csv'

time_start = time.time()

filter_threshold = int(sys.argv[1])
support_threshold = int(sys.argv[2])
input_file_path = sys.argv[3]
output_file_path = sys.argv[4]

sc = pyspark.SparkContext()

raw = sc.textFile(input_file_path).map(lambda x : x.split(','))
title = raw.take(1)
data_deal = raw.filter(lambda x: x[0] != title[0][0]).map(lambda x: [str(str(x[0][1:6])+str(x[0][8:-1])),int(x[1][1:-1]),int(x[5][1:-1])])
data_deal_2 = data_deal.map(lambda x:[str(x[0]+'-'+str(x[1])), x[2]])

data_output = data_deal_2.collect()
with open("customer_product.csv",'w') as file:
    writer = csv.writer(file)
    writer.writerow(['DATE-CUSTOMER_ID', 'PRODUCT_ID'])
    for d in data_output:
        writer.writerow(d)

#df = pyspark.sql.SparkSession.createDataFrame(data_deal_2, ['DATE-CUSTOMER_ID', 'PRODUCT_ID'])
#df.write.csv("customer_product.csv", sep=',', header=True)



#data_deal_2.saveAsTextFile("customer_product.csv")

#data = data_deal_2.collect()
#print(data[0:5])

data_new = sc.textFile("customer_product.csv").map(lambda x: x.split(','))
title = data_new.take(1)
data_RDD = data_new.filter(lambda x: x[0] != title[0][0]).map(lambda x: [x[0], x[1]]).groupByKey().mapValues(lambda x: set(x)).filter(lambda x: len(x[1]) > filter_threshold).map(lambda x:x[1])

baskets_num = data_RDD.count()


def collect_single_item(original_chunk_baskets):
    single_items = set()
    for basket in original_chunk_baskets:
        for item in basket:
            single_items.add(frozenset({item}))
            #single_items.sort()
    return single_items

def filter_to_frequent(candidate_itemset, chunk_baskets, support_threshold):
    frequent_itemset_count ={}
    for candidate in candidate_itemset:
        for chunk in chunk_baskets:
            if candidate.issubset(chunk):
                if candidate in frequent_itemset_count.keys():
                    frequent_itemset_count[candidate] +=1
                else:
                    frequent_itemset_count[candidate] = 1
    frequent_itemset = set()
    for key,value in frequent_itemset_count.items():
        if value >= support_threshold:
            frequent_itemset.add(key)
    return frequent_itemset

def get_bigger_size(size, low_size_itemset):
    generated_candidate = set()
    for one in low_size_itemset:
        for two in low_size_itemset:
            candi = one.union(two)
            #candi = candi.sort()
            if len(candi) == size:
                sub_candi = itertools.combinations(frozenset(candi), size-1)
                for sub in sub_candi:
                    if frozenset(sub) not in low_size_itemset:
                        break
                    else:
                        generated_candidate.add(frozenset(candi))
                #generated_candidate.add(frozenset(candi))
    #copy_generated_candidate = generated_candidate.copy()
    #for y in low_size_itemset:
        #print("low_size:",y)
    #for c in generated_candidate.copy():
        #print("c_in_generated:",c)
        #c_sub = itertools.combinations(c, size-1)
        #for sub in c_sub:
            #print("sub:",frozenset(sub))
            #if frozenset(sub) not in low_size_itemset:
                #generated_candidate.remove(c)
    return generated_candidate

def apriori_algorithm(original_chunk_baskets, lowscaled_support_threshold):
    basic_itemset = collect_single_item(original_chunk_baskets)
    filtered_basic_itemset = filter_to_frequent(basic_itemset,original_chunk_baskets,lowscaled_support_threshold)
    size = 2
    all_frequent_candidate = {}
    while len(filtered_basic_itemset) !=0:
        all_frequent_candidate[size -1] = filtered_basic_itemset
        generated_itemset = get_bigger_size(size, filtered_basic_itemset)
        filtered_basic_itemset = filter_to_frequent(generated_itemset,original_chunk_baskets,lowscaled_support_threshold)
        size = size + 1
    return all_frequent_candidate

#original_chunk_baskets = data_RDD.collect()

#basic_itemset = collect_single_item(original_chunk_baskets)
#filtered_basic_itemset = filter_to_frequent(basic_itemset,original_chunk_baskets,1)
#size = 2
#all_frequent_candidate = {}
#all_frequent_candidate[size -1] = filtered_basic_itemset
#generated_itemset = get_bigger_size(size, filtered_basic_itemset)
#filtered_basic_itemset_2 = filter_to_frequent(generated_itemset,original_chunk_baskets,1)

#output_local_frequent = apriori_algorithm(original_chunk_baskets,1)

def son_phase_1(chunk):
    original_chunk_baskets = list(chunk)
    lowscaled_support_threshold = math.ceil((len(original_chunk_baskets)/baskets_num)*support_threshold)
    output_local_frequent = apriori_algorithm(original_chunk_baskets,lowscaled_support_threshold)
    output_local_candidate = set()
    for i in output_local_frequent.values():
        for j in i:
            output_local_candidate.add(j)
    return output_local_candidate
    #return output_local_frequent
    

phase_1_rdd = data_RDD.mapPartitions(son_phase_1).distinct()
phase_1 = phase_1_rdd.collect()

#Sort the order inside tuple
phase_1_sorted = list(map(list,phase_1))
for p in phase_1_sorted:
    p.sort()

with open(output_file_path, 'w') as f:
    f.write("Candidates:\n")
    #f.write(str(phase_2))

    len_collect = set()
    for i in phase_1_sorted:
        len_collect.add(len(i))
    len_max = max(len_collect)

    write_list_1 = []
    for items in phase_1_sorted:
        if len(items) ==1:
            write_list_1.append(tuple(items)[0])
    write_list_1.sort()
    w_len = len(write_list_1)
    for i in write_list_1:
        if write_list_1.index(i) != len(write_list_1)-1:
            f.write("('"+ str(i)+"'),")
        else:
            f.write("('"+ str(i)+"')")
    f.write("\n\n")
                  
    length = 2
    while length <= len_max:
        write_list = []
        write_str = ''
        for itemset in phase_1_sorted:
            if len(itemset)==length:
                write_list.append(tuple(itemset))#tuple
        write_list.sort()    
        f.write(str(write_list)[1:-1] + "\n\n")
        length +=1
        
        

def son_phase_2(chunk):
    final_count_filter = {}
    original_chunk_baskets = list(chunk)
    for c in phase_1:
        for b in original_chunk_baskets:
            if c.issubset(b):
                if c in final_count_filter.keys():
                    final_count_filter[c] += 1
                else:
                    final_count_filter[c] = 1
    item_count = []
    for key, value in final_count_filter.items():
        item_count.append((key,value))
    return item_count

phase_2_rdd = data_RDD.mapPartitions(son_phase_2).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] >=support_threshold).map(lambda x: x[0])
phase_2 = phase_2_rdd.collect()

phase_2_sorted = list(map(list,phase_2))
for q in phase_2_sorted:
    q.sort()
    
#print("OUtput:",phase_2)
with open(output_file_path, 'a') as f:
    f.write("Frequent Itemsets:\n")
    #f.write(str(phase_2))
    len_collect = set()
    for i in phase_2_sorted:
        len_collect.add(len(i))
    len_max = max(len_collect)

    write_list_1 = []
    for items in phase_2_sorted:
        if len(items) ==1:
            write_list_1.append(tuple(items)[0])
    write_list_1.sort()
    w_len = len(write_list_1)
    for i in write_list_1:
        if write_list_1.index(i) != len(write_list_1)-1:
            f.write("('"+ str(i)+"'),")
        else:
            f.write("('"+ str(i)+"')")
    f.write("\n\n")         
                    
    length = 2
    while length <= len_max:
        write_list = []
        write_str = ''
        for itemset in phase_2_sorted:
            if len(itemset)==length:
                write_list.append(tuple(itemset))#tuple
        write_list.sort()    
        f.write(str(write_list)[1:-1] + "\n\n")
        length +=1

    
    
time_end = time.time()
print("Duration:{0:.2f}".format(time_end - time_start))

#/opt/spark/spark-3.1.2-bin-hadoop3.2/bin/spark-submit --executor-memory 4G --driver-memory 4G task2.py 20 50 '../resource/asnlib/publicdata/ta_feng_all_months_merged.csv' output_task2.py


