# Databricks notebook source
data = [('James','Java'),
  ('James','Python'),
  ('James','Python'),
  ('Anna','PHP'),
  ('Anna','Javascript'),
  ('Maria','Java'),
  ('Maria','C++'),
  ('James','Scala'),
  ('Anna','PHP'),
  ('Anna','HTML')
]

# Create DataFrame
df = spark.createDataFrame(data,schema=["name","languages"])
df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import collect_list, explode

result_df = df.groupBy("name").agg(collect_list("languages").alias("languages"))
display(result_df)

# COMMAND ----------

original_form_df = result_df.select("name", explode("languages"))
display(original_form_df)

# COMMAND ----------

from pyspark.sql.functions import collect_set, concat_ws

df_without_duplicates = df.groupBy("name").agg(concat_ws(", ",collect_set("languages").alias("languages")))
display(df_without_duplicates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Window Function Practice

# COMMAND ----------

simpleData = (
    ("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )
 
columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Row Number

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, col


windowSpec = Window.partitionBy("department").orderBy(col("salary").desc())
result_df = df.withColumn("row_number", row_number().over(windowSpec))\
    .withColumn("rank", rank().over(windowSpec))\
    .withColumn("dense_rank", dense_rank().over(windowSpec) )

display(result_df)
result_df.where("rank = 1").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q. Left Rotation and Right Rotation of a String

# COMMAND ----------

# Python3 program for Left 
# Rotation and Right
# Rotation of a String

# In-place rotates s towards left by d
def leftrotate(s, d):
    tmp = s[d : ] + s[0 : d]
    return tmp
  
# In-place rotates s 
# towards right by d
def rightrotate(s, d):
  
   return leftrotate(s, len(s) - d)

# Driver code
if __name__=="__main__":
    
    str1 = "GeeksforGeeks"
    print(leftrotate(str1, 2))
 
    str2 = "GeeksforGeeks"
    print(rightrotate(str2, 2))

# This code is contributed by Rutvik_56


# COMMAND ----------

tmp = str1[2 : ] + str1[0 : 2]
print(tmp)

# COMMAND ----------

input_str = "thebigdatashow"
k = 2
length = len(input_str) 
result = input_str[(length-k):] + input_str[:length-k ]
print(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Q. Check for balanced parentheses in Python

# COMMAND ----------

# Python3 code to Check for 
# balanced parentheses in an expression
def check(expression):
	
	open_tup = tuple('({[')
	close_tup = tuple(')}]')
	map = dict(zip(open_tup, close_tup))
	queue = []

	for i in expression:
		if i in open_tup:
			queue.append(map[i])
		elif i in close_tup:
			if not queue or i != queue.pop():
				return "Unbalanced"
	if not queue:
		return "Balanced"
	else:
		return "Unbalanced"

# Driver code
string = "{[]{()}}"
print(string, "-", check(string))

string = "((()"
print(string,"-",check(string))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Q. Find all triplets with zero sum

# COMMAND ----------

# A simple Python 3 program
# to find three elements whose
# sum is equal to zero

# Prints all triplets in
# arr[] with 0 sum


def findTriplets(arr, n):

    found = False
    for i in range(0, n-2):

        for j in range(i+1, n-1):

            for k in range(j+1, n):

                if (arr[i] + arr[j] + arr[k] == 0):
                    print(arr[i], arr[j], arr[k])
                    found = True

    # If no triplet with 0 sum
    # found in array
    if (found == False):
        print(" not exist ")


# Driver code
arr = [0, -1, 2, -3, 1]
n = len(arr)
findTriplets(arr, n)

# This code is contributed by Smitha Dinesh Semwal
