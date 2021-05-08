import findspark  
findspark.init()  

import pyspark # only run after findspark.init()  

from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.appName('rssi_percs_25_eval_rssl_task2').getOrCreate()

#Create DataFrame  
part1_df = spark.read.csv(r"C:\Users\Dell\Downloads\ProdApt\data\client_stats_sample_0225part1.csv", inferSchema = True, header = True)

#count on large dataset will be slow, so used cache for better perfomace on subsequent call
part1_df.cache()

part1_df_mod = part1_df.select("freq_band","client_mac","rssi_percs_25")

#PART-1 : Solutions
part1_df_tot_rec_count = part1_df_mod.count()
part1_df_5G_freq_device_count = part1_df_mod.filter("freq_band" == '5G').count()
part1_df_2p4G_freq_device_count = part1_df_tot_rec_count-part1_df_5G_freq_device_count


part1_df_5G_freq_device_prctg =  part1_df_5G_freq_device_count/part1_df_tot_rec_count
part1_df_2p4G_freq_device_prctg  =  part1_df_2p4G_freq_device_count/part1_df_tot_rec_count

stats_data = [(part1_df_5G_freq_device_prctg, part1_df_2p4G_freq_device_prctg)]

stats_schema = StructType([ StructField("5G_device_percentage",StringType(),True),
               StructField("2.4G_device_percentage",StringType(),True) ])

stats_df = spark.createDataFrame(data=stats_data,schema=stats_schema)

# Save modified dataframe to HDFS as output_stats.csv
stats_df.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('/output_stats.csv')



#PART-2 : Solutions
part2_df_5G_rssi_gt_75db_count = part1_df_mod.filter("freq_band" == '5G').filter("rssi_percs_25" > -75).count()
part2_df_2p4G_rssi_gte_60db_count = part1_df_mod.filter("freq_band" == '2.4G').filter("rssi_percs_25" >= -60).count()

part2_df_5G_freq_device_prctg = (part2_df_5G_rssi_gt_75db_count + part2_df_2p4G_rssi_gte_60db_count)/part1_df_tot_rec_count


part2_df_2p4G_rssi_lt_60db_count = part1_df_mod.filter("freq_band" == '2.4G').filter("rssi_percs_25" < -60).count()
part2_df_5G_rssi_lte_75db_count = part1_df_mod.filter("freq_band" == '5G').filter("rssi_percs_25" <= -75).count()

part2_df_2p4G_freq_device_prctg = (part2_df_2p4G_rssi_lt_60db_count + part2_df_5G_rssi_lte_75db_count)/part1_df_tot_rec_count


post_BS_activation_stats_data = [(part2_df_5G_freq_device_prctg, part2_df_2p4G_freq_device_prctg)]

post_BS_activation_stats_data_schema = StructType([ StructField("5G_device_percentage_post_BS_activation",StringType(),True),
                                       StructField("2.4G_device_percentage_post_BS_activation",StringType(),True) ])

post_BS_activation_stats_df = spark.createDataFrame(data=post_BS_activation_stats_data,schema=post_BS_activation_stats_data_schema)

# Save modified dataframe to HDFS as output_stats.csv
post_BS_activation_stats_df.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('/post_BS_activation_output_stats.csv')

part2_df_5G_freq_device_count = part2_df_5G_freq_device_count + 
device_df = part1_df_mod.filter("rssi_percs_25" > -85 ).filter("rssi_percs_25" < -65)