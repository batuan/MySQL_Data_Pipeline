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
    mysql_properties = {
        "url": "jdbc:mysql://{}:{}/{}".format(os.getenv('mysql_hostname'),os.getenv('mysql_port'), os.getenv('mysql_db')),
        "driver": "com.mysql.jdbc.Driver",
        "user": os.getenv('mysql_user'),
        "password": os.getenv("mysql_password"),
        "dbtable": os.getenv("mysql_dbtable")
    }

    # Read the CSV file into a DataFrame
    df = spark.read.format("csv") \
        .option("header", "true") \
        .load("../employees - infos badge à mettre à jour.csv")

    # Perform join with the employee table to get the employee_id
    employee_df = spark.read.jdbc(mysql_properties["url"], "employee", properties=mysql_properties)

    # Join
    df_with_employee_id = df.join(
        employee_df, (df["employee_first_name"] == employee_df["first_name"]) & (df["employee_last_name"] == employee_df["lastName"]))\
            .select(col("badge_serial_number").alias("badge_serial_number"), col("id").alias("employee_id"))

    # Write the DataFrame to the MySQL database
    df_with_employee_id.write.mode("append") \
        .jdbc(mysql_properties["url"], mysql_properties["dbtable"], properties=mysql_properties)

    # Stop the SparkSession
    spark.stop()
