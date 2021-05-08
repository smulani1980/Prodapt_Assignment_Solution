import findspark  
findspark.init()  

import pyspark # only run after findspark.init()  

from pyspark.sql import SparkSession

from pyspark.sql.functions import avg

from pyspark_dist_explore import hist
import matplotlib.pyplot as plt



spark = SparkSession.builder.appName('rssi_percs_25_with_avg_tx_rxWeightedPhyRate').getOrCreate()

#Create DataFrame  
part1_df = spark.read.csv(r"C:\Users\Dell\Downloads\ProdApt\data\client_stats_sample_0225part1.csv", inferSchema = True, header = True)

#Perform SQL queries  
part1_df_mod = part1_df.select("rssi_percs_25","txWeightedPhyRate","rxWeightedPhyRate")
part1_df_mod = part1_df_mod.filter("rssi_percs_25" > -85 ).filter("rssi_percs_25" < -65)
part1_df_mod = part1_df_mod.fillna(0, subset=['txWeightedPhyRate', 'rxWeightedPhyRate'])
part1_df_mod = part1_df_mod.groupBy("rssi_percs_25").agg(avg("txWeightedPhyRate").alias("avg_txWeightedPhyRate"),
                                                         avg("rxWeightedPhyRate").alias("avg_rxWeightedPhyRate"))
part1_df_mod = part1_df_mod.select("rssi_percs_25","avg_txWeightedPhyRate","avg_rxWeightedPhyRate
part1_df_mod = part1_df_mod.orderBy("rssi_percs_25", ascending=True)

part1_df_mod.show()

#plot the histogram for both txWeightedPhyRate & rxWeightedPhyRate
#use the pyspark_dist_explore package to leverage the matplotlib hist function for Spark DataFrames

fig, ax1, ax2 = plt.subplots(1,2)
hist(ax1, part1_df_mod.select('txWeightedPhyRate'), bins = 21, color=['red'])
hist(ax2, part1_df_mod.select('rxWeightedPhyRate'), bins = 21, color=['blue'])


# Save modified dataframe to HDFS as output.csv
part1_df_mod.write.format('csv').option('header',True).mode('overwrite').option('sep',',').save('/output.csv')


# Design spec for data engineer to store data on elastick search 
# Step(1) Instead of storing CSV to HDFS ,write CSV to Elastrict Search using (a) Spark-ES Configurations and (b) writeToIndex() code
# Step(2) Once we have our DataFrame ready, all we need to do is import org.elasticsearch.spark.sql._ and invoke the .saveToEs() method on it.
# Step(3) Index your data into Elasticsearch before you can create a graph in Graphana