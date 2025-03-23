import datetime as dt
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import logging  # Import the logging module
from make_dimension import makeDateDimension, makeTimeDimension, makeUserAgentDimension, makeLocationDimension,makeFactTableDim

# define (local) folders where files will be found / copied / staged / written
WorkingDirectory = "/opt/airflow/dags/w3c"
LogFiles = WorkingDirectory + "/LogFiles/"
StagingArea = WorkingDirectory + "/StagingArea/"
StarSchema = WorkingDirectory + "/StarSchema/"

# Create a String for a BASH command that will extract / sort unique IP 
# addresses from one file, copying them over into another file
uniqueIPsCommand = "sort -u " + StagingArea + "RawIPAddresses.txt > " + StagingArea + "UniqueIPAddresses.txt"

# Another BASH command, this time to extract unique Date values from one file into another
uniqueDatesCommand = "sort -u " + StagingArea + "RawDates.txt > " + StagingArea + "UniqueDates.txt"

uniqueUserAgentCommand = "sort -u " + StarSchema + "DimUserAgentTable.txt > " + StarSchema + "DimUniqueUserAgentTable.txt"
# Another BASH command, this time to copy the Fact Table that is produced from the Staging area to the resultant folder
copyFactTableCommand = "cp " + StagingArea + "FactTable.txt " + StarSchema + "FactTable.txt"

# prior to any processing, make sure the expected directory structure is in place for files
try:   
   os.makedirs(WorkingDirectory, exist_ok=True)
except:
   print("Can't make WorkingDirectory")
try:
   os.makedirs(LogFiles, exist_ok=True)
except:
   print("Can't make LogFiles folder") 
try: 
   os.makedirs(StagingArea, exist_ok=True)
except:
   print("Can't make StagingArea folder") 
try:
   os.makedirs(StarSchema, exist_ok=True)
except:
   print("Can't make StarSchema folder") 


# Copy data from a given log file into the staging area.
# The content of the log file will be appended to a file
# in the staging area that will eventually contain the data
# from ALL log files combined.
# Note: the log files may contain comment lines in them, 
# e.g., beginning with a '#' hash. These are ignored / not
# copied to the output file during the copy process
def CopyDataFromLogFileIntoStagingArea(nameOfLogFile):
    print('Copying content from log file', nameOfLogFile)
    logging.warning('Cleaning '+nameOfLogFile)
    #print (uniqCommand)

    # extra check: get the suffix from the log filename, e.g., '.log'
    suffix = nameOfLogFile[-3:len(nameOfLogFile)]

    # if file suffix is 'log', i.e., it is indeed a 'log' file
    # and not anything else (no point in introducing unwanted data here)
    if (suffix=="log"):
    
        # we have a log file to process
        # it may contain 14 cols or 18 cols
        # we will append the file content into an output file in the StagingArea

        # open output file(s) in the StagingArea to append data into, i.e., to append
        # the lines of data we are reading in from the log file. There is an output file 
        # to store the data being read from the 14-col log files, and another output file
        # to store data being read from the 18-col log files
        OutputFileFor14ColData = open(StagingArea + 'OutputFor14ColData.txt', 'a')
        OutputFileFor18ColData = open(StagingArea + 'OutputFor18ColData.txt', 'a')

        # open the input file, i.e, the log file we want to read data from
        InFile = open(LogFiles + nameOfLogFile, 'r')
    
        # read in the lines / content of the log file
        Lines = InFile.readlines()
        # for each line read in from the log file, one at a time
        for line in Lines:
            if line.startswith("#"):
                if line.startswith("#Fields:"):
                    # Extract column count from #Fields row
                    expected_columns = len(line.strip().split(" ")[1:])
                    print(f"Expected column count based on #Fields: {expected_columns}")
                continue
            else:    
                # it is a valid line to process 
                # check how many cols are in the data
                # each column in a row may be separated by a space
                # split the next line of data in the file based on spaces 
                 
                # if the length of the split is 14
                if (expected_columns==14):
                     # list of indices to remove because we are not interested in them
                    indices_to_remove = [2, 3, 5, 6, 7, 11, 12]
                     # clean line of data by removing unneeded columns
                    clean_line=CleanLine(indices_to_remove, line, expected_columns)
                   # write line of data into the output file for 14-col data
                    OutputFileFor14ColData.write(f"{clean_line}\n")
                if (expected_columns==18):
                       # list of indices to remove because we are not interested in them
                       indices_to_remove = [2, 3, 5, 6, 7, 13, 14]
                        # clean line of data by removing unneeded columns
                       clean_line=CleanLine(indices_to_remove, line, expected_columns)
                       # write line of data into the output file for 18-col data
                       OutputFileFor18ColData.write(f"{clean_line}\n") 


"""
    Cleans a single log line by removing specified columns based on their indices.

    This function splits a log line into columns (based on spaces), removes the columns 
    at specified indices, and then joins the remaining columns back into a single string.

 Note:
    - Assumming that the input line contains at expected_columns
    - If the input line contains fewer columns than expected, the function will ignore the line without raising an error.
    """
def CleanLine(indices_to_remove,line, expected_columns):
    # Split the line into columns
    columns = line.strip().split(" ")
    if(len(columns)!=expected_columns):
        # ignore lines that dosen't match the file header
        logging.warning("Fault: unrecognised column number at line: " + line)
        print ("Fault: unrecognised column number at line: " + line)
        return ""
     # Filter the columns by removing the ones at the specified indices
    cleaned_columns = [col for i, col in enumerate(columns) if i not in indices_to_remove]

    # Join the cleaned columns back into a single string
    cleaned_line = " ".join(cleaned_columns)

    return cleaned_line
  
 
# clear the content of any files in the staging area - opening the file
# with 'write' mode instead of 'append' mode will effectively truncate
# its content to zero
def EmptyOutputFilesInStagingArea():
    open(StagingArea + 'OutputFor14ColData.txt', 'w')
    open(StagingArea + 'OutputFor18ColData.txt', 'w')
    open(StagingArea + 'FactTableFor14.txt', 'w')
    open(StagingArea + 'FactTableFor18.txt', 'w')

# copy (the content of) all log files into the staging area
def CopyLogFilesToStagingArea():
   # get a list of all files in the 'log' files folder - these are the 'raw'
   # input to our process
   arr = os.listdir(LogFiles)
   
   # if no files are found
   if not arr:
      # display an error notification
      print('No files found in Log Files folder')

   # clear/empty the content of output files in the staging area
   # where we will be copying the content of the log files into
   EmptyOutputFilesInStagingArea()

   # for each log file 'f' found in the log files folder
   for f in arr:
       # copy the content of this next log file 'f' over into the output file(s) in the staging area
       CopyDataFromLogFileIntoStagingArea(f)
       
# add / append data from the 14-col files into the Fact table
def Add14ColDataToFactTable():
    # open output file that contains all 14-col data aggregated
    InFile = open(StagingArea + 'OutputFor14ColData.txt','r')

    # open Fact table to write / append into
    OutFact1 = open(StagingArea + 'FactTableFor14.txt', 'a')
    # write header row into the fact table
  
    OutFact1.write("Date,Time,cs-uri-stem,IP,Browser,sc-status,ResponseTime\n")

    # read in all lines of data from input file (14-col data)
    Lines= InFile.readlines()

    # for each line in the input file
    for line in Lines:
        # split line into columns
        Split=line.strip().split(" ")

        # among other things, the line of data has the following: Date,Time,Browser,IP,ResponseTime
        # do some reformatting of the browser field if required, to remove ',' chars from it
        browser = Split[4].replace(",","")

        # create line of text to write to output file, made up of the following: Date,Time,Browser,IP,ResponseTime
        OutputLine =Split[0] + "," + Split[1] +  "," + Split[2] +  "," + Split[3] + "," + browser + "," + Split[5] + "," + Split[6] + "\n"

        # write line of text to output file
        OutFact1.write(OutputLine)

# add / append data from the 18-col files into the Fact table
def Add18ColDataToFactTable():
    # open output file that contains all 18-col data aggregated
    InFile = open(StagingArea + 'OutputFor18ColData.txt','r')

    # open Fact table to write / append into
    OutFact2 = open(StagingArea + 'FactTableFor18.txt', 'a')
    OutFact2.write("Date,Time,Uri-stem,IP,Browser,Status,ResponseTime,Cookie,Referrer,Sc-bytes,Cs-bytes\n")

    # read in all lines of data from input file (18-col data)
    Lines = InFile.readlines()
    # Regular expression to capture browser name and version
   
    # for each line in the input file
    for line in Lines:
        # split line into columns
        Split = line.strip().split(" ")
        #TODO:clean data if needed
        # do some reformatting of the browser field
        # Extract browser details 
        Browser = Split[4].replace(",","")

        # create line of text to write to output file, made up of the following: Date,Time,Browser,IP,ResponseTime
        Out = Split[0] + "," + Split[1] +  "," + Split[2] +  "," + Split[3] + "," + Browser + "," + Split[7] + "," + Split[10]  + "," + Split[6]  + "," + Split[8]  + "," + Split[9] + "\n"

        # write line of text to output file
        OutFact2.write(Out)

# build the fact table
def BuildFactTable():
    # add / append data from 14-col log files into Fact table
    Add14ColDataToFactTable()

    # add / append data from 18-col log files into Fact table
    Add18ColDataToFactTable()
#TODO: 2.	Transform: Converting, processing, and aggregating that data into a
#unified form that is relevant to our business analytical needs
#Transformation: changing the type or structure
#TODO: create  Dimension table for the IP

# # copy / extract all IP addresses from the Fact Tables 14 &18
# # eventually, these will be used to create and populate
# # a Dimension table for the IP / Location. This is just
# # a first stage in processing to acheive this. Initially,
# # ALL ip addresses will be copied from the Fact table 
# # which means some of them may be duplicates / non-unique.
# # This will be resolved in a subsequent stage
# def extractDataFromFactTable(): 
#     # open the fact tables (as it contains all rows of data)
#     InFile1 = open(StagingArea + 'FactTableFor14.txt', 'r')
    
#     InFile2 = open(StagingArea + 'FactTableFor18.txt', 'r')
    


# copy / extract all IP addresses from the Fact Table
# eventually, these will be used to create and populate
# a Dimension table for the IP / Location. This is just
# a first stage in processing to acheive this. Initially,
# ALL ip addresses will be copied from the Fact table 
# which means some of them may be duplicates / non-unique.
# This will be resolved in a subsequent stage
def getIPsFromFactTable():
    # open file to write IP data into
    OutputFile = open(StagingArea + 'RawIPAddresses.txt', 'w')
    # open file to write IP data into
    # open fact table 
    InFile1 = open(StagingArea + 'FactTableFor14.txt', 'r')
    writeOutputToFile(InFile1, OutputFile, 3)

    InFile2 = open(StagingArea + 'FactTableFor18.txt', 'r')
    writeOutputToFile(InFile2, OutputFile, 3)

# copy / extract all dates from the Fact Table
# eventually, these will be used to create and populate
# a Dimension table for the dates. This is just
# a first stage in processing to acheive this. Initially,
# ALL dates will be copied from the Fact table 
# which means some of them may be duplicates / non-unique.
# This will be resolved in a subsequent stage
def getDatesFromFactTable():
    # open output file to write dates into
    OutputFile = open(StagingArea + 'RawDates.txt', 'w')
    InFile1 = open(StagingArea + 'FactTableFor14.txt', 'r')
    writeOutputToFile(InFile1, OutputFile, 0)

    InFile2 = open(StagingArea + 'FactTableFor18.txt', 'r')
    writeOutputToFile(InFile2, OutputFile, 0)

# copy / extract all time
def getTimeFromFactTable():
    # open output file to write dates into
    TimeFile = open(StagingArea + 'Time.txt', 'w')
    InFile1 = open(StagingArea + 'FactTableFor14.txt', 'r')
    writeOutputToFile(InFile1, TimeFile, 1)

    InFile2 = open(StagingArea + 'FactTableFor18.txt', 'r')
    writeOutputToFile(InFile2, TimeFile, 1)
  
# copy / extract all time
def getUserAgentFromFactTable():
    # open output file to write dates into
    OutputFile = open(StagingArea + 'UserAgent.txt', 'w')
    InFile1 = open(StagingArea + 'FactTableFor14.txt', 'r')
    writeOutputToFile(InFile1, OutputFile, 4 )

    InFile2 = open(StagingArea + 'FactTableFor18.txt', 'r')
    writeOutputToFile(InFile2, OutputFile, 4)

 # copy / extract all referrer
def getStatusCodeFromFactTable():
    # open output file to write dates into
    statusFile = open(StagingArea + 'StatusCode.txt', 'w')
    InFile1 = open(StagingArea + 'FactTableFor14.txt', 'r')
    writeOutputToFile(InFile1, statusFile, 5)

    InFile2 = open(StagingArea + 'FactTableFor18.txt', 'r')
    writeOutputToFile(InFile2, statusFile, 5)
    

  # copy / extract all referrer
def getReferrerFromFactTable():
    # open output file to write dates into
    referrerFile = open(StagingArea + 'Referrer.txt', 'w')
    InFile1 = open(StagingArea + 'FactTableFor18.txt', 'r')
    writeOutputToFile(InFile1, referrerFile, 7)

    """
    This function reads details from an input file, extracts the desired values
    and writes the extracted information to an output file.
    """
def writeOutputToFile(InFile, OutputFile, Index):
    # get the IP address & write it to the file
     # read all lines from input file
    Lines = InFile.readlines()
    firstLine=True
    # for each line / row of data
    for line in Lines:
       if firstLine == True:
            # ignore this line, but record we have found it now
            firstLine = False
       else:
            # split the line into its parts
            Split = line.split(",")
            value = Split[Index] + "\n"
            # write value to output file 
            if value!='-':
                OutputFile.write(value)


# the DAG - required for Apache Airflow
dag = DAG(                                                     
   dag_id="Process_W3_Data",                          
   schedule_interval="@daily",                                     
   start_date=dt.datetime(2023, 3, 6), 
   catchup=False,
)

## TASKS
# A python operator to copy data from the log files into the staging area
task_CopyLogFilesToStagingArea = PythonOperator(
   task_id="task_CopyLogFilesToStagingArea",
   python_callable=CopyLogFilesToStagingArea, 
   dag=dag,
)

# A python operator to copy / extract IP address data from the Fact table
task_getIPsFromFactTable = PythonOperator(
    task_id="task_getIPsFromFactTable",
    python_callable=getIPsFromFactTable,
    dag=dag,
)

# A python operator to copy / extract date information from the Fact table
task_getDatesFromFactTable = PythonOperator(
    task_id="task_getDatesFromFactTable",
    python_callable=getDatesFromFactTable,
    dag=dag,
)

# A python operator to copy / extract browser data from the Fact table
task_getUserAgentFromFactTable = PythonOperator(
    task_id="task_getUserAgentFromFactTable",
    python_callable=getUserAgentFromFactTable,
    dag=dag,
)

# A python operator to copy / extract referrer information from the Fact table
task_getReferrerFromFactTable = PythonOperator(
    task_id="task_getReferrerFromFactTable",
    python_callable=getReferrerFromFactTable,
    dag=dag,
)

# A python operator to copy / extract status code information from the Fact table
task_getStatusCodeFromFactTable = PythonOperator(
    task_id="task_getStatusCodeFromFactTable",
    python_callable=getStatusCodeFromFactTable,
    dag=dag,
)
# A python operator to copy / extract status code information from the Fact table
task_getTimeFromFactTable = PythonOperator(
    task_id="task_getTimeFromFactTable",
    python_callable=getTimeFromFactTable,
    dag=dag,
)

# # A python operator to build the Location Dimension based on IP addresses
task_makeLocationDimension = PythonOperator(
    task_id="task_makeLocationDimension",
    python_callable=makeLocationDimension,
    dag=dag,
)

# A python operator to build the Fact table from data contained in the log files
task_BuildFactTable = PythonOperator(
   task_id="task_BuildFactTable",
   python_callable= BuildFactTable,
   dag=dag,
)

# A python operator to build the Date Dimension based on date information
task_makeDateDimension = PythonOperator(
   task_id="task_makeDateDimension",
   python_callable=makeDateDimension, 
   dag=dag,
)

# A python operator to build the Date Dimension based on time information
task_makeTimeDimension = PythonOperator(
   task_id="task_makeTimeDimension",
   python_callable=makeTimeDimension, 
   dag=dag,
)


# A python operator to build the Date Dimension based on date information
task_makeUserAgentDimension = PythonOperator(
   task_id="task_makeUserAgentDimension",
   python_callable=makeUserAgentDimension, 
   dag=dag,
)

# A bash operator that will transform the complete list of original IP addresses into
# a file containing only unique IP addresses
task_makeUniqueIPs = BashOperator(
    task_id="task_makeUniqueIPs",
    bash_command=uniqueIPsCommand,
    dag=dag,
)

task_makeUniqueUserAgent=  BashOperator(
    task_id="task_makeUniqueUserAgent",
    bash_command=uniqueUserAgentCommand,
    dag=dag,
)
 
# A bash operator that will transform the complete list of original dates into
# a file containing only unique dates
task_makeUniqueDates = BashOperator(
    task_id="task_makeUniqueDates",
    bash_command=uniqueDatesCommand,
    dag=dag,
)

# a bash operator that will copy the Fact table from its temporary location in the 
# Staging Area (where it is used during the creation of Dimension tables) into the Star Schema location
task_makeFactTableDim = PythonOperator(
     task_id="task_makeFactTableDim",
     python_callable=makeFactTableDim,
 #     bash_command="cp /home/airflow/gcs/data/Staging/OutFact1.txt /home/airflow/gcs/data/StarSchema/OutFact1.txt",
     dag=dag,
)
 
# usually, you can set up your ETL pipeline as follows, where each task follows on from the previous, one after another:
# task1 >> task2 >> task3  

# if you want to have tasks working together in parallel (e.g., if we wanted the IP address processing
# to be occurring at the same time as the Date processing was occurring), we need to define the 
# pipeline in a different way, making clear which tasks are 'downstream' of each other (occurring after) 
# or 'upstream' of each other (required to occur before)
# for example, we could define a structure as follows:
#TODO: check ETL pipeline/parallel
#                                                        -> task_getDatesFromFactTable -> task_makeUniqueDates -> task_makeDateDimension
# task_CopyLogFilesToStagingArea -> task_BuildFactTable                                                                                     -> task_copyFactTable
#                                                        -> task_extractFromFactTable -> task_makeUniqueIPs -> task_makeLocationDimension
#
# In the above, we could say the following: task_copyFactTable is 'downstream' of task_makeDateDimension
# OR, we could say that task_makeDateDimension is 'upstream' of task_copyFactTable
# 
# There are methods we can call to set up these dependencies. E.g., for the above, we could do:
# task_copyFactTable.set_upstream(task_makeDateDimension)
# OR
# task_makeDateDimension.set_downstream(task_copyFactTable)
#
# If TaskA has both TaskB and TaskC upstream of it, TaskA will only commence when BOTH TaskB and TaskC have completed before it.
#

 
#task_makeLocationDimension >> task_copyFactTable
#task_makeLocationDimension >> task_copyFactTable

#task_makeUserAgentDimension >> task_makeUniqueUserAgent
#task_makeUniqueDates >> task_makeDateDimension
#task_makeUniqueIPs >> task_makeLocationDimension


#task_getUserAgentFromFactTable >> task_makeUserAgentDimension
#task_getTimeFromFactTable >> task_makeTimeDimension 

#task_getDatesFromFactTable >> task_makeUniqueDates
#task_getIPsFromFactTable >> task_makeUniqueIPs

#parallel
task_BuildFactTable >> task_makeFactTableDim
task_BuildFactTable >>  task_getStatusCodeFromFactTable
task_BuildFactTable >>  task_getReferrerFromFactTable
task_BuildFactTable >>  task_getUserAgentFromFactTable >> task_makeUserAgentDimension >> task_makeUniqueUserAgent
task_BuildFactTable >>  task_getTimeFromFactTable >> task_makeTimeDimension
task_BuildFactTable >>  task_getDatesFromFactTable  >> task_makeUniqueDates >> task_makeDateDimension
task_BuildFactTable >> task_getIPsFromFactTable >> task_makeUniqueIPs >> task_makeLocationDimension

task_CopyLogFilesToStagingArea >> task_BuildFactTable
