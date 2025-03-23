from db_operations import insert_ip_details ,insert_date_details, insert_user_agent_details, insert_time_details
import pandas as pd
import math
from datetime import datetime
import json
from datetime import datetime 
import requests
from user_agents import parse 
import openpyxl
from write_to_excel import writeToExcel

# define (local) folders where files will be found / copied / staged / written
WorkingDirectory = "/opt/airflow/dags/w3c"
LogFiles = WorkingDirectory + "/LogFiles/"
StagingArea = WorkingDirectory + "/StagingArea/"
StarSchema = WorkingDirectory + "/StarSchema/"

# define days of the week - used in routine(s) below
Days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

#TODO: LOAD 
# create / build a dimension table for the date information
def makeDateDimension():
    # open file that contains dates extracted from the fact table, subsequently made unique
    InDateFile = open(StagingArea + 'UniqueDates.txt', 'r')   

    data=[]
    data.append(["Date", "Year", "Month", "Day", "DayofWeek", "Quarter"])
    
    # write a header row into the output file for constituent parts of the date
    with open(StarSchema + 'DimDateTable.txt', 'w') as file:
        file.write("Date,Year,Month,Day,DayofWeek,Quarter\n")
    
    # get lines of data from input file (where each 'line' will be a Date string)
    Lines = InDateFile.readlines()
    
    # for each line / date
    for line in Lines:
        # remove any new line that may be present
       line = line.strip()
       if line:
            # try the following
            try:
                # get the date from the line of text, e.g., year, month, day
                date = datetime.strptime(line,"%Y-%m-%d").date()
                 # get the weekday as a string, e.g., 'Monday', 'Tuesday', etc.
                weekday = Days[date.weekday()]

                # create line of text to write to output file with the different components of the date
                # each line / row will have the original date string [key], year, month, day, weekday
                #date =  date)
                year = str(date.year)
                month = str(date.month)
                day = str(date.day)
                quarter = str(math.ceil(int(month) / 3))

                out = f"{date},{year},{month},{day},{weekday},{quarter}\n"
                
                #insert_date_details(date,year,month,day,weekday,quarter)    
                # Append row to data list
                data.append([date, year, month, day, weekday, quarter])
                
                # write / append the date information to the output file     
                       
                with open(StarSchema + 'DimDateTable.txt', 'a') as file:
                    file.write(out)
                    
            except Exception as e:
                print(f"Error with Date: {e}") # report error in case of exception

    writeToExcel("DateDim", data)


    
# create / build a dimension table for the 'time' information derived from time
def makeTimeDimension(): 
    # open file in staging area that contains the time extracted from the Fact table
    InTimeFile = open(StagingArea + 'Time.txt', 'r')   
    # open output file to write date dimension data into
    OutputDateFile = open(StarSchema + 'DimTimeTable.txt', 'w')
 
    data=[] 
    data.append(["Time","Hour","Minute","Second"])
 
    # write a header row into the output file for constituent parts of the date
    with OutputDateFile as file:
       file.write("Time,Hour,Minute,Second\n")

    # get lines of data from input file (where each 'line' will be a Date string)
    Lines = InTimeFile.readlines()
    
    # for each line / date
    for line in Lines:
        # remove any new line that may be present
       line = line.strip()
       if line:
            # try the following
            try:
                # get the time from the line of text, e.g., hour, min, second
                time = datetime.strptime(line, "%H:%M:%S").time()

                # create line of text to write to output file with the different components of the date
                # each line / row will have the original date string [key], year, month, day, weekday
                #date =  date)
                hour = str(time.hour)
                minute = str(time.minute)
                second = str(time.second) 
                out= f"{time},{hour},{minute},{second}\n"
                 #insert_time_details(time,hour,minute,second)    
                data.append([time,hour,minute,second])

                # write / append the date information to the output file            
                with open(StarSchema + 'DimTimeTable.txt', 'a') as file:
                    file.write(out)
            except Exception as e:
                print(f"An error occurred: {e}") # report error in case of exception
    writeToExcel("TimeDim",data)      

# create / build a dimension table for the 'user agent' information derived from user agent
def makeUserAgentDimension(): 
    # open file in staging area that contains the unique IP addresses extracted from the Fact table
    InFile = open(StagingArea + 'UserAgent.txt', 'r')

    data=[]
    data.append(["Browser","Version","OS","Device_type","Language","Rendering Engine"])
    
    # write a header row into the output file for constituent parts of agent
    with open(StarSchema + 'DimUserAgentTable.txt', 'w') as file:
               file.write("Browser,Version,OS,Device_type,Language,Rendering Engine\n")
    

    # read in lines / user agent from file
    Lines = InFile.readlines()

    # for each line / user agent in the file
    for line in Lines:
        line = line.replace("\n","")
 
        if (len(line) > 0):
             user_agent = parse(line)
             browser=user_agent.browser.family or '-'
             version_string=user_agent.browser.version_string or '-'
             os= user_agent.os.family or '-'
             device_type  ="Mobile" if user_agent.is_mobile else "Tablet" if user_agent.is_tablet else "PC"
             engine="Trident" if "MSIE" in line else "Unknown"
             outputLine = browser + "," + version_string + "," +os + "," + device_type+ "," + engine + "\n"
             data.append([browser,version_string,os,device_type,engine])
             #insert_user_agent_details(browser,version_string,os,device_type,engine)    
                # write / append the line to the output file
             with open(StarSchema + 'DimUserAgentTable.txt', 'a') as file:
                    file.write(outputLine)
               
    # Save to Excel
    writeToExcel("AgentDim", data)

# create / build a dimension table for the 'location' information derived from IP addresses
def makeLocationDimension():
    
    # define path to the file that will store the location dimension
    #DimTablename = StarSchema + 'DimIPLoc.txt'
    # open file in staging area that contains the unique IP addresses extracted from the Fact table
    InFile = open(StagingArea + 'UniqueIPAddresses.txt', 'r')
 
    data=[]
    data.append(["IP","country_code","country_name","city","state","postcode","latitude","longitude"])
    

    # write a header row into the output file for constituent parts of the location
    with open(StarSchema + 'DimIPLoc.txt', 'w') as file:
               file.write("IP, country_code, country_name, city, state, postcode, lat, long\n")
    
    # read in lines / IP addresses from file
    Lines = InFile.readlines()

    # for each line / IP address in the file
    for line in Lines:
        # remove any new line from it
        line = line.strip()
        if line: # automatically checks if the string is non-empty
            # define URL of API to send the IP address to, in return for detailed location information
            request_url = 'https://geolocation-db.com/jsonp/' + line 
            # Send request and decode the result
            try:
                response = requests.get(request_url)
                resultResponse = response.content.decode()
            except:
                print ("Error response from geolocation API: " + resultResponse)
            
            # process the response
            try:
                # Clean the returned string so it just contains the location data for the IP address
                resultJson = resultResponse.split("(")[1].strip(")")
                # Convert the location data into a dictionary so that individual fields can be extracted
                result  = json.loads(resultJson)
              
                # create line of text to write to output file representing the location Dimension
                # each line / row will have the original IP address [key], country code, country name, city, lat, long
                country_code = result["country_code"]
                country =  result["country_name"]
                city = result["city"]
                latitude = str(result["latitude"])
                longitude = str(result["longitude"])
                state = result["state"]
                postcode = str(result["postal"])
                print(result,country_code,country,city, longitude,latitude,state,postcode)
                outputLine = f"{line},{country_code},{country},{city},{state},{postcode},{latitude},{longitude}\n"
                data.append([line,country_code, country,city, state, postcode, latitude, longitude])

                #insert_ip_details(line,country,city, state, postcode, latitude, longitude)    
                # write / append the line to the output file
                with open(StarSchema + 'DimIPLoc.txt', 'a') as file:
                    file.write(outputLine)
            except json.JSONDecodeError as e:
                print("JSON decoding error:", e)
            except Exception as e:
                print ("An error occurred:", e)
        # Save to Excel
    writeToExcel("LocationDim", data)  
    
    
def makeFactTableDim(): 
    # write a header row into the output file for constituent parts of the location
    with open(StarSchema + 'FactTable.txt', 'w') as file:
               file.write("Date,Time,Uri-stem,IP,Browser,Status,ResponseTime,Cookie,Referrer,Sc-bytes,Cs-bytes\n")
    
    data=[]
    data.append(["Date","Time","Uri-stem","IP","Browser","Status","ResponseTime","Cookie", "Referrer", "Sc-bytes","Cs-bytes" ])
    makeFactTableDim14()
    makeFactTableDim18() 
    
def makeFactTableDim14():    
    InFile = open(StagingArea + 'FactTableFor14.txt', 'r')
    
    data=[] 
     
    Lines = InFile.readlines()
    for line in Lines:
        # remove any new line from it
        line = line.strip()
        if line: # automatically checks if the string is non-empty 
            outputLine = f"{line}\n"
            split = line.strip().split(",")
            data.append(split)

    #insert_ip_details(line,country,city, state, postcode, latitude, longitude)    
    # write / append the line to the output file
    with open(StarSchema + 'FactTable.txt', 'a') as file:
        file.write(outputLine)
    # Save to Excel
    writeToExcel("FactTable", data)  
        
def makeFactTableDim18():    
    InFile = open(StagingArea + 'FactTableFor18.txt', 'r')
    
    data=[] 
     
    Lines = InFile.readlines()
    for line in Lines:
        # remove any new line from it
        line = line.strip()
        if line: # automatically checks if the string is non-empty 
            outputLine = f"{line}\n"
            split = line.strip().split(",")
            data.append(split)

    #insert_ip_details(line,country,city, state, postcode, latitude, longitude)    
    # write / append the line to the output file
    with open(StarSchema + 'FactTable.txt', 'a') as file:
        file.write(outputLine)
    # Save to Excel
    writeToExcel("FactTable", data)  
        
        
        