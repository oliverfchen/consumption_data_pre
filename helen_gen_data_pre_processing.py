#/this code is prepared for CISCO IoT project
#/to create 500 pv sites data based on real data for IR829 router
#/the seed data of pv "production" is using SolarEdge.com download 1 year data
#/the consumer "consumption" values is using data from data.gov.au
#/the contains 25 real user electricity consumption data is combined
#/with real pv data, and calculate other values from it
#/the pv site scale up factor using 1.5 ~ 7 KW base on the real data on 3 KW output
#/the scale up factor selected skewed right of general distibution
#/the data of gov data change to 2016 - 2018 to match pv data
#/the data interval changed from 30 minutes to 15 minutes by insert average value between 2 data points
#/the gov data discard "generation" in 'TYPE" column, only used data which "genaral" in "TYPE" column
#/the time formart change to match each other 
#/the merge table is based on the "DateTime" column as key
#/created by Fang Chen
#/26/Aug/2018 in UTS 
#/location in building 11 level 8 IoT lab
#/04/March/2019 re-do , check coding error
#/the total 24 consumer consumption data norminalized and interval in 15 minutes, unit is KWh
import pandas as pd
import csv
import re
import os
import datetime

def make_csv_raw(user_raw_data):
    respondent_df = []
    columns = ['date', 'time', 'consumption']
    respondent_df = pd.DataFrame(columns=columns)
    date_time = []
    consumption_ = []  
    row_list = []
    for index, date in enumerate(user_raw_data['OUTPUTDATE']):       
        row_dic = {}
        for time in time_interval:
            date_time_ = date + " " + time + ":00"
            date_time = pd.to_datetime(date_time, format='%d/%m/%Y %H%M%S')
            if user_raw_data.iloc[index]['TYPE'] == 'general':
                consumption_general = user_raw_data.iloc[index][time]
                if index < len(user_raw_data)-1  and user_raw_data.iloc[index+1]['TYPE'] == 'controlled load':
                    consumption_controlled_load = user_raw_data.iloc[index+1][time]
                    #print 'consumption_controlled_load', consumption_controlled_load,
                else:
                    consumption_controlled_load = 0
                consumption = consumption_controlled_load + consumption_general
                dic = {'date': date, 'time': time, 'consumption': consumption}  
                row_list.append(dic)
            else:
                continue
    respondent_df = pd.DataFrame(row_list)
    return respondent_df

def make_insert_15(respondent_df):
    respondent_df_new = []
    row_list = []
    columns = ['date', 'time', 'date_time', 'consumption']
    respondent_df_new = pd.DataFrame(columns=columns)
    for index in range(len(respondent_df)):        
        date = respondent_df.iloc[index]['date']
        time = respondent_df.iloc[index]['time']
        date_time = date + ' ' + time
        consumption = respondent_df.iloc[index]['consumption']
        dic = {'date': date, 'time': time, 'date_time': date_time, 'consumption': consumption}
        row_list.append(dic)
        if index < len(respondent_df) - 1:
            next_consumption = respondent_df.iloc[index + 1]['consumption']
        everage_consumption = (consumption + next_consumption) / 2.0
        time = time[0:3] + str(int(time[-2:]) + 15)
        date_time = date + ' ' + time
        dic = {'date': date, 'time': time, 'date_time': date_time, 'consumption': everage_consumption}
        row_list.append(dic)
    respondent_df_new = pd.DataFrame(row_list)
    return respondent_df_new



if __name__ == '__main__':
    
    raw_data = '.\scr\elec_consump_benchmark-data.gov.au\electricityconsumptionbenchmarkssurveydataaergovhack_v1.csv'
    df = pd.read_csv(raw_data)
    row_data_columes = df.columns
    time_interval = []
    new_colume = []
    for colume in row_data_columes:
        temp = re.sub('WH','', colume)
        temp = re.sub('E_', '', temp)
        temp = re.sub('_', '' ,temp)
        if temp.isdigit() is True:
            temp = pd.to_datetime(temp, format='%H%M')
            temp = temp.strftime('%H:%M')
            time_interval.append(temp)
        new_colume.append(temp)

    df.columns = new_colume
    



    df_= df.T
  
            

    respondents_list = df.respondent.unique()

    outputdates = df_.iloc[1]
    types = df_.iloc[2]   
    #data_directory = os.getcwd() + '/temp_2/'
    #if not os.path.exists(data_directory):
     #       os.makedirs(data_directory)
# step 1 cProfile 1 create row files 25
    data_directory = './v2_data_R/cProfile1_v2_R/'
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    for user in respondents_list:
        raw_data = df[((df.respondent == user))]
        file_name = data_directory + 'cProfile1_v2_R' + str(user) + '.csv'
        raw_data.to_csv(file_name, encoding='utf-8', index=False) 
    print ("Step 1, cProfile1 cerate.")
  

  

#step 2 general + control load -> cProfile2
print ("Step 2, add Control_load data to consumption.")
data_directory = './v2_data_R/cProfile2_v2_R/'
if not os.path.exists(data_directory):
    os.makedirs(data_directory)
for user in respondents_list:
    raw_file_name = './v2_data_R/cProfile1_v2_R/cProfile1_v2_R' + str(user) + '.csv'
    user_raw_data = pd.read_csv(raw_file_name)
    print ('\nuser name:',user)
    for index in range(len(user_raw_data)):
        
        if 0 < index < len(user_raw_data)-1  and user_raw_data.iloc[index+1]['TYPE'] == 'controlled load':
            print ('\rcontrol load index:',index,end=" ")

            for time in time_interval:

                consumption_controlled_load = user_raw_data.iloc[index+1][time]

                original_consumption = user_raw_data.iloc[index][time]

                user_raw_data.iloc[index, user_raw_data.columns.get_loc(time)] = original_consumption + consumption_controlled_load

    
    new_file = user_raw_data[((user_raw_data['TYPE'] == 'general'))]
    new_file_name = data_directory + 'cProfile2_V2_R' + str(user) + '.csv'
    new_file.to_csv(new_file_name, encoding='utf-8', index=False)
print ("Step 2, cProfile 2 creation completed.")

#step 3, average to get one year data
def average_data(user):
    data_path = './v2_data_R/cProfile3_v2_R/'
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    raw_file =  './v2_data_R/cProfile2_v2_R/cProfile2_V2_R' + str(user) + '.csv'
    raw_data = pd.read_csv(raw_file)
    # change data from 2012 to 2014
    new_year =[]
    new_date = []
    for n in range(len(raw_data)):
        day = raw_data.iloc[n]["OUTPUTDATE"][:-5]
        year = raw_data.iloc[n]["OUTPUTDATE"][-4:]
        new_date.append(day)
        new_year.append(year)
    new_date_ = pd.DataFrame(new_date)
    new_year_ = pd.DataFrame(new_year)
    raw_data['year'] = new_year_
    raw_data['date'] = new_date_
    data_2012 = raw_data[((raw_data.year == '2012'))]
    data_2013_ = raw_data[((raw_data.year == "2013"))]
    data_2013 = data_2013_[((data_2013_.TYPE == 'general'))]  #only keep TYPE general
    data_2014 = raw_data[((raw_data.year == "2014"))]
    global time_column
    time_column = data_2013.columns[3:-2]
    for n in range(len(data_2013)):
        this_day = data_2013.iloc[n]['date']       
        for m in range(len(data_2012)):
            check_day = data_2012.iloc[m]['date']
            if this_day == check_day:
                #add to data in 2012 and average
                for time in time_column:
                    new_value = (data_2013.iloc[n][time] + data_2012.iloc[m][time]) / 2.0
                    data_2013.iloc[n, data_2013.columns.get_loc(time)] = new_value
           
    for n in range(len(data_2013)):  
        this_day = data_2013.iloc[n]['date']   
        for m in range(len(data_2014)):
            check_day = data_2014.iloc[m]['date']
            if this_day == check_day:
                #add to data in 2014 and average
                for time in time_column:
                    new_value = ((data_2013.iloc[n][time] * 2.0) + data_2014.iloc[m][time]) / 3.0
                    data_2013.iloc[n, data_2013.columns.get_loc(time)] = new_value           
    return data_2013


print ("Step 3. Average daily consumption data.")
data_directory = './v2_data_R/cProfile3_v2_R/'
if not os.path.exists(data_directory):
    os.makedirs(data_directory)
for user in respondents_list:
    averaged_data = average_data(user)
    file_name = data_directory + 'cProfile3_average_v2_R' + str(user) + '.csv'
    averaged_data.to_csv(file_name, encoding='utf-8', index=False)
    print ("file created", file_name)
print ("Step 3 completed")

import pandas as pd
import csv
import re
import os
import datetime

#part 4,normalise data

print ("Part 4, normalise data ")
def get_max_min(user):

    max_day = []
    min_day = []

    #for user in respondents_list:
    file_path = './v2_data_R/'
    user_file = file_path + 'cProfile3_v2_R/cProfile3_average_v2_R' + str(user) + '.csv'
    raw_data = pd.read_csv(user_file)
    
    #get every day's max min values
    for row in range(len(raw_data)):
        row_ = raw_data.loc[row][3:-2]     #get one day data
        day_consumption_max = max(row_)
        day_consumption_min = min(row_)
        max_day.append(day_consumption_max)
        min_day.append(day_consumption_min)
    return max_day

def normal_data(user, max_day_user):
    #for user in respondents_list:
    file_path = './v2_data_R/'
    raw_file = file_path + 'cProfile3_v2_R/cProfile3_average_v2_R' +str(user) + '.csv'
    raw_data = pd.read_csv(raw_file)

    for n in range(len(raw_data)):
        for time in time_column:
            if max_day_user != 0:
                normal_value = raw_data.iloc[n][time] / max_day_user
                raw_data.iloc[n, raw_data.columns.get_loc(time)] = normal_value  
    return raw_data

data_path = './v2_data_R/cProfile4_v2_R/'
if not os.path.exists(data_path):
    os.makedirs(data_path)
maxmax = []
for user in respondents_list:    
    max_day = get_max_min(user)
    maxmax.append(max(max_day))
    print ("maxmum value of user in a year", max(max_day))
    raw_data = normal_data(user, max(max_day))
    raw_data['max_day'] = max_day
    file_name = data_path + 'cProfile4_v2_ref_normal_R' + str(user) + '.csv'
    raw_data.to_csv(file_name, encoding='utf-8', index=False)
    print (file_name, 'Normalised file v2 saved.')
print ("Part 4 completed.")

#save max day to file
maxmax_df = []
maxmax_df = pd.DataFrame(columns = [ 'respondent','max'])
maxmax_df['respondent'] = respondents_list
maxmax_df['max'] = maxmax
data_path = './v2_data_R/max/'
if not os.path.exists(data_path):
    os.makedirs(data_path)
file_name = data_path + 'cProfile_v2_MAX_day.csv'
maxmax_df.to_csv(file_name, encoding='utf-8', index=False)
print ("max data saved. ")
print ("pre_processing completed.")

#change unit from KWh to KWh in 15 intervaal (by devide 2
#create 15 minutes data by average 2 nearby value
#save new data to cProfile5_v2_ref_normal_15m.csv
import numpy
new_data_path = './v2_data_R/cProfile5_v2_R/'
data_path = './v2_data_R/cProfile4_v2_R/'
if not os.path.exists(new_data_path):
    os.makedirs(new_data_path)
#load cProfile_v2_ref_normal_R.csv
for user in respondents_list:
    file_name = data_path + 'cProfile4_v2_ref_normal_R' + str(user) + '.csv'
    df = pd.read_csv(file_name)
    new_df = []
    new_df = pd.DataFrame()
    new_df = df.iloc[:,0:3]
    column = df.columns[0:3]
    
    for index, time_ in enumerate(time_column):
        #new_df[time_].append(df[time_])
        new_df[time_] = df[time_] / 2.0   #change kwh value from 30 to 15 mins
        column = numpy.append(column,time_) 
        
        time_15 = time_.split(':')[0] +':'+ str(int(time_.split(':')[1]) + 15)  #create 15 mins time
        if index < len(time_column) -1:
            new_df[time_15] = (df[time_column[index]] + df[time_column[index + 1]]) / 4.0
            column = numpy.append(column,time_15)
        else:
            new_df[time_15] = (df[time_column[index]] + df[time_column[0]]) / 4.0 #average 2 values, changed to 15 kwh
            column = numpy.append(column,time_15)
            
    #print ('\r',column)
    new_df.columns = column
    new_file_name = new_data_path + str(user) + '_2013.csv'
    
    new_df.to_csv(new_file_name,encoding = 'utf-8', index = False)
    print (new_file_name, 'saved')
print ('cProfile5_v2_15_R completed')