from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
from dotenv import load_dotenv
load_dotenv('../.env')

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CSV to MySQL") \
    .config("spark.jars", "./mysql-connector-java-5.1.34_1.jar") \
    .getOrCreate()

if __name__ == "__main__":
# Configure MySQL connection properties
    query = "select id, first_name, lastName from employee where (first_name, lastName) \n" +\
    "IN ( SELECT first_name, lastName FROM employee GROUP BY first_name, lastName having count(*)=1)"
    

    mysql_properties = {
        "url": "jdbc:mysql://{}:{}/{}".format(os.getenv('mysql_hostname'),os.getenv('mysql_port'), os.getenv('mysql_db')),
        "driver": "com.mysql.jdbc.Driver",
        "user": os.getenv('mysql_user'),
        "password": os.getenv("mysql_password")
    }

    # Read the CSV file into a DataFrame
    df = spark.read.format("csv") \
        .option("header", "true") \
        .load("../employees - infos badge à mettre à jour.csv")

    # Perform join with the employee table to get the employee_id
    employee_df = spark.read \
                    .format("jdbc") \
                    .option("driver", "com.mysql.jdbc.Driver") \
                    .option("url", "jdbc:mysql://{}:{}/{}".format(os.getenv('mysql_hostname'),os.getenv('mysql_port'), os.getenv('mysql_db'))) \
                    .option("user", os.getenv('mysql_user')) \
                    .option("password", os.getenv("mysql_password")) \
                    .option("query", query)\
                    .load()

   
    # Join
    df_with_employee_id = employee_df.join(
        df, (df["employee_first_name"] == employee_df["first_name"]) & (df["employee_last_name"] == employee_df["lastName"]))\
            .select(col("badge_serial_number").alias("badge_serial_number"), col("id").alias("employee_id"))
    print(len(df_with_employee_id.collect()))
    # Write the DataFrame to the MySQL database
    df_with_employee_id.write.mode("append") \
        .jdbc(mysql_properties["url"], os.getenv("mysql_dbtable"), properties=mysql_properties)

    # Stop the SparkSession
    spark.stop()
