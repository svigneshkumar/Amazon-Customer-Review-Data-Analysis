A) Passphrase-less SSH

B) Hadoop Configurations common on all nodes

C) NameNode Specific Configurations

D) Datanode Specific Configurations

E) Start Hadoop Cluster

   hdfs namenode -format
   $HADOOP_HOME/sbin/start-dfs.sh
   $HADOOP_HOME/sbin/start-yarn.sh
   $HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

******** Data Set Link ********
https://s3.amazonaws.com/amazon-reviews-pds/tsv/index.txt

Note: 
There are so many categories of data in the above mentioned link. Download any dataset under "US REVIEWS DATASET".
If you want to test the code using more than one dataset, You just need to combine them.

F) Commands 

a) Upload the entire data set to the input directory 

$ hdfs namenode -format
$ sbin/start-all.sh
$ sbin/mr-jobhistory-daemon.sh start historyserver

b) Upload input file/s from input directory to HDFS

$ hdfs dfs -mkdir -p input
$ hdfs dfs -put input/* input


d) Compile the java files and make a jar file and upload the jar file to HDFS
**** You can find the jar files in the zip folder ****


e) Run the 4 programs

i) % of products in a category with highest rating

$ hadoop jar wc.jar WordCount /user/input/ output1

$ hdfs dfs -cat output1/* > output1.txt

ii) SENTIMENTAL ANALYSIS ON REVIEWS
***The program displays a category of data and compile the products according to their votes and differentiate the customer review positive or negative based on some key words***

$ hadoop jar votes.jar votes /user/input/ output2

$ hdfs dfs -cat output2/* > output2.txt

iii) % of products in a category with lowest rating

$ hadoop jar lr.jar LowestRating /user/input/ output3

$ hdfs dfs -cat output3/* > output3.txt

iv) CUSTOMERS WITH REVIEW COUNT CATEGORICALLY
***This program displays the customers and counts the number of reviews they gave and display according to the category***

$ hadoop jar cr.jar CustomerReviews /user/input/ output4

$ hdfs dfs -cat output4/* > output4.txt


