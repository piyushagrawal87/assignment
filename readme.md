#                                                  Data Challenge - Solution
The data provided is from a fake grocery store called ACME. There are 12 CSV files in total: 10 trans fact files, 1 location file and 1 product file. How the data can be integrated is pretty evident in the field names. Collector key that have positive values are customers that are in the loyalty program. There may be missing sales and units in some trans fact files that need to be replaced with zero value.

## Challenges
### Step 1: Data Gathering & Ingestion
uploadfiles_to_cloud.py pushes files from local to google cloud bucket in an autmated way. This code requires uses a hard-coded folder location (which obviously can be parameterized) with an optional files name as an input parameter to identify the all files in a folder/a given file in a folder. This program overwrites the files if already available in cloud storage or uploads a copy, if not available. In production, this script can be scheduled either using a cron job or with any scheduler (oozie) to push data in an automated manner.

### Step 2: Secure the data
The cloud storage used for storing the input files is private and access to it limited to only authorized inviduals. Google Cloud Storage always encrypts data on the server side, before it is written to disks, so data at rest is encrypted here. gcloud API for python implements "Encryption in transit" which protects data if communications are intercepted while data moves between local/source and the cloud provider or between two services. This protection is achieved by encrypting the data before transmission; authenticating the endpoints; and decrypting and verifying the data on arrival. For example, Transport Layer Security (TLS) is often used to encrypt data in transit for transport security, and Secure/Multipurpose Internet Mail Extensions (S/MIME) is used often for email message security.

### Step 3: Prepare & Cleanse the data in memory
processData_cloud.py implements this steps. Following steps are cover the implementation of preparing and cleaning the data in memory
* Reading all trans fact files (1 to 10).
* Fixing inconsistencies in the header (different sequence/ different header names).
* combining trans fact files to make one dataframe.
* Reading location and product files.
* Combining trans fact file with location file with location key and with product file with product key to prepare one large dataframe having transactions, their store locations and product details all in it.
* This dataframe is then brought into spark-memory so that subsequent transformations/actions can be executed quickly.

### Step 4: Transform the data in memory
#### which provinces and stores are performing well and how much are the top stores in each province performing compared with the average store of the province
* Top provinces
This section of code displays top 3 provinces as per total sales.
* Top Stores
This section of code displays top 3 stores as per total sales.
* Top store to Average Store
This section calculates, sales of top stores in each province, avegrage sales of each store in a province and then provides % performance KPI for top stores against average store.

### Step 5: Transform the data in memory
#### loyalty program vs non-loyalty customers and what category of products is contributing to most of ACME’s sales
* loyalty_vs_nonloyalty
This section calculates total sales for loyalty customers and total sales of non loyalty customers and calculates % performance of former over later.
* Categories and sales
This section calculates top 10 categories and total sales.
**NOTE: After matching the transactions with Products and Locations, it turned out that lots of transactions had nulls in categories which turns out to be the highest total sales category (null) so 2nd to 10th should be considered Top 9 categories.**

### Step 6: Transform the data in memory
#### Determine the top 5 stores by province and top 10 product categories by department
* Top 5 store by province
This section calculates top 5 stores for each given province
* Top 10 Product categories per department
This section calculates top 10 categories for each given departments.

### Step 7: Bring in the database
#### Store the results of your analytics into a database engine in a cloud platform of tour choice. 
Each section outlines above stores the results in a MYSQL database on Google cloud platform.

### Step 8: Display your analytics
#### Create a dashboard showing absolute numbers, trend for last 12 months and year over year (YoY) for the following metrics:
•	sales, units, distinct count of transactions, distinct count of collectors
•	Display the metrics for loyalty customers, non-loyalty customers and overall
•	Consider how the president of ACME might use the dashboard and any further questions he may have that can be answered through the use of the dashboard


sales, units, distinct count of transactions, distinct count of collectors are calculated month over month which can then be used to calculate YOY numbers in any BI tool.
dashboard_new.pbis contains a power BI dashboard with Total sales, Total Units, Total unique txns and Total unique collectors for Loyalty, NonLoyalty and Overall. This dashboard also has these number trending MOM.

Other pages of the dahsboard are used to display th results of steps 4 to 7.


### Other details
* File requirements.txt outlines the package names which were installed on the machine with hadoop cluster.
* test_2 and test_3 are some unit tests which were used to test the functions written in processData_cloud file. Please note that I have not considered writing an extremely modularized code due to time constraint and have included these tests to showcase unit testing feature incorporated for a pyspark script.

Unit tests can be executed by CDing into the code folder and executing "python -m pytest -vv". Unit tests use pytest-spark package. 


### Continuous Integration
The framework implemented above doesn't implement continuous integration fully but has all parts/components which are pivotal for CI implementation of spark data pipelines. The idea is to use Git hooks to trigger a batch script on push into a personal branch. The bash script will SSH into the cluster, execute some git pull command to download the pushed code in a folder, run unit test (outlines above) and respond Pass/Fail based on unit test result. Git push can then be merged to Master for successful unit tests or rolledback on failed unit test. 

This process needed more exploration and time committement which was not possible considering procument of technologies/tools for the assignment and completing the other KPI asks in the assignment itself were extermely timeconsuming.

**Thanks a lot for reading till the end! I hope you enjoyed learning more about how went about this assignment. I would love to hear any feedback on the work done! 

**Thanks again!!





