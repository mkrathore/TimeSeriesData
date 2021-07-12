# TimeSeriesData
Perform data transformation to keep track of individual sessions and session -timeouts

# Problem Statement
# Part A
    Description : You are given a time series data, which is a clickstream of user activity. Perform Sessionization
    on the data {as per the session definition given below} and generate session ids.
    Expected Steps:
    1. Create in an input file with the data given below in any flat file format, preferably csv.
    2. Read the input data file into your program
    3. Use spark batch {PySpark/Spark-Scala} to add an additional column with name session_id and generate the session
    id's based on the following logic:
        Session expires after inactivity of 30 minutes; because of inactivity, no clickstream will be generated.
        Session remains active for a maximum duration of 2 hours (i.e., after every two hours a new session starts).
    4. Save the resultant data (original data, enriched with Session IDs) in a Parquet file format.
 # Given Data
    Timestamp             User_id
    2021-05-01T11:00:00Z      u1
    2021-05-01T13:13:00Z      u1
    2021-05-01T15:00:00Z      u2
    2021-05-01T11:25:00Z      u1
    2021-05-01T15:15:00Z      u2
    2021-05-01T02:13:00Z      u3
    2021-05-03T02:15:00Z      u4
    2021-05-02T11:45:00Z      u1
    2021-05-02T11:00:00Z      u3
    2021-05-03T12:15:00Z      u3
    2021-05-03T11:00:00Z      u4
    2021-05-03T21:00:00Z      u4
    2021-05-04T19:00:00Z      u2
    2021-05-04T09:00:00Z      u3
    2021-05-04T08:15:00Z      u1
  # Expected Output
    TimeStamp             User_id  Session_id(<userid>_<session_number>)
    2021-05-01T11:00:00Z    u1     For example, u1_s1
    2021-05-01T13:13:00Z    u1     For example, u1_s2
    2021-05-01T15:00:00Z    u2     For example, u2_s1
    2021-05-01T11:25:00Z    u1     Please derive based on given logic
    2021-05-01T15:15:00Z    u2     Please derive based on given logic
    2021-05-01T02:13:00Z    u3     Please derive based on given logic
    2021-05-03T02:15:00Z    u4     Please derive based on given logic
    2021-05-02T11:45:00Z    u1     Please derive based on given logic
    2021-05-02T11:00:00Z    u3     Please derive based on given logic
    2021-05-03T12:15:00Z    u3     Please derive based on given logic
    2021-05-03T11:00:00Z    u4     Please derive based on given logic
    2021-05-03T21:00:00Z    u4     Please derive based on given logic
    2021-05-04T19:00:00Z    u2     Please derive based on given logic
    2021-05-04T09:00:00Z    u3     Please derive based on given logic
    2021-05-04T08:15:00Z    u1     Please derive based on given logic
# Part B
    Description: In addition to the above result, consider the following requirement/scenario and give the results
      Get the Number of sessions generated for each day.
      Total time spend by a user in a day.
    Here are the guidelines and instructions for the expected solution:
      Write all the queries for the different requirements in Spark-sql.
      Think in the direction of using partitioning, bucketing, etc.
# Note
    Please do not use direct Spark-sql in section A.
   
