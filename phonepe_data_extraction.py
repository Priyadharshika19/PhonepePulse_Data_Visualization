import pandas as pd
import os
import json
import psycopg2
db=psycopg2.connect(host='localhost', user='postgres', password=318327, port=5432, database="PhonePe")
cursor=db.cursor()

#table 1
def agg_trans_file(t_path,agg_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}
    for state in agg_trans_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        #lst_year = []
        for year in year_list:
            #lst_year.append(year)
            year_path=state_path+year+"/"
            Q_file=os.listdir(year_path)
            #print(Q_file)

            for data in Q_file:
                #lst_Q.append(data)
                file_path=year_path+data
                file_content= open(file_path, "r")
                file_data=json.load(file_content)
                #print(file_data)
                try:
                    for i in  file_data["data"]["transactionData"]:
                        trans_name=i["name"]
                        trans_count=i["paymentInstruments"][0]["count"]
                        trans_amount=i["paymentInstruments"][0]["amount"]
                        Dict_trans['Transaction_type'].append(trans_name)
                        Dict_trans['Transaction_count'].append(trans_count)
                        Dict_trans['Transaction_amount'].append(trans_amount)
                        Dict_trans['State'].append(state)
                        Dict_trans['Year'].append(year)
                        Dict_trans['Quarter'].append(int(data.strip('.json')))
                except:
                    pass

    df_agg_trans=pd.DataFrame(Dict_trans)
    df_agg_trans['State'] = df_agg_trans['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'})
    df_agg_trans.to_csv('agg_trans.csv', index=False)
    #print(df_agg_trans)
    return df_agg_trans

def a_trans(trans_df):
    cursor.execute("DROP table IF EXISTS agg_trans;")
    db.commit()
    cursor.execute("create table agg_trans (State varchar(100), Year int, Quarter int, Transaction_type varchar(100), Transaction_count int, Transaction_amount real)")
    for i,row in trans_df.iterrows():
        sql = "INSERT INTO agg_trans VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        db.commit()

#table 2
def agg_user_file(t_path,agg_user_list):
    Dict_user = {'State': [], 'Year': [], 'Quarter': [], 'Brand': [], 'Count': [], 'Percentage': []}
    for state in agg_user_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        for year in year_list:
            year_path=state_path+year+"/"
            Q_file=os.listdir(year_path)
            #print(Q_file)
            for data in Q_file:
                file_path=year_path+data
                file_content= open(file_path, "r")
                file_data=json.load(file_content)
                if(file_data["data"]["usersByDevice"]==None):
                    pass
                else:
                    try:
                        for i in file_data["data"]["usersByDevice"]:
                            user_brand= i["brand"]
                            user_count= i["count"]
                            user_percentage=i["percentage"]
                            Dict_user['Brand'].append(user_brand)
                            Dict_user['Count'].append(user_count)
                            Dict_user['Percentage'].append(user_percentage)
                            Dict_user['State'].append(state)
                            Dict_user['Year'].append(year)
                            Dict_user['Quarter'].append(int(data.strip('.json')))
                    except:
                        pass
    df_agg_user = pd.DataFrame(Dict_user)
    df_agg_user['State'] = df_agg_user['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'})
    df_agg_user.to_csv('agg_user.csv', index=False)
    #print(df_agg_user)
    return df_agg_user

def a_user(user_df):
    cursor.execute("DROP table IF EXISTS agg_user;")
    db.commit()
    cursor.execute("create table agg_user (State varchar(100), Year int, Quarter int, Brand varchar(100), Transaction_Count int, Percentage float)")
    for i,row in user_df.iterrows():
        sql = "INSERT INTO agg_user VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        db.commit()


#table 3
def map_trans_file(t_path,map_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'Name': [], 'Count': [], 'Amount': []}
    for state in map_trans_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        for year in year_list:
            year_path=state_path+year+"/"
            Q_file=os.listdir(year_path)
            #print(Q_file)
            for data in Q_file:
                file_path=year_path+data
                file_content= open(file_path, "r")
                file_data=json.load(file_content)
                #print(file_data)
                try:
                    for i in  file_data["data"]["hoverDataList"]:
                        trans_name=i["name"]
                        trans_count=i["metric"][0]["count"]
                        trans_amount=i["metric"][0]["amount"]
                        Dict_trans['Name'].append(trans_name)
                        Dict_trans['Count'].append(trans_count)
                        Dict_trans['Amount'].append(trans_amount)
                        Dict_trans['State'].append(state)
                        Dict_trans['Year'].append(year)
                        Dict_trans['Quarter'].append(int(data.strip('.json')))
                except:
                    pass
    df_map_trans=pd.DataFrame(Dict_trans)
    df_map_trans['State'] = df_map_trans['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'})
    df_map_trans.to_csv('agg_trans.csv', index=False)
    #print(df_agg_trans)
    return df_map_trans

def m_trans(trans_df):
    cursor.execute("DROP table IF EXISTS map_trans;")
    db.commit()
    cursor.execute("create table map_trans (State varchar(100), Year int, Quarter int, District varchar(100), Transaction_Count int, Transaction_Amount real)")
    for i,row in trans_df.iterrows():
        sql = "INSERT INTO map_trans VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        db.commit()


#table 4
def map_user_file(t_path,map_user_list):
    Dict_user = {'State': [], 'Year': [], 'Quarter': [], 'District': [], 'RegisteredUsers': [], 'AppOpens': []}
    for state in map_user_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        for year in year_list:
            year_path=state_path+year+"/"
            Q_file=os.listdir(year_path)
            #print(Q_file)
            for data in Q_file:
                file_path=year_path+data
                file_content= open(file_path, "r")
                file_data=json.load(file_content)
                if(file_data["data"]["hoverData"]==None):
                    pass
                else:
                    try:
                        for i in file_data["data"]["hoverData"].items():
                            user_dist= i[0]
                            user_reg= i[1]["registeredUsers"]
                            user_app=i[1]["appOpens"]
                            Dict_user['District'].append(user_dist)
                            Dict_user['RegisteredUsers'].append(user_reg)
                            Dict_user['AppOpens'].append(user_app)
                            Dict_user['State'].append(state)
                            Dict_user['Year'].append(year)
                            Dict_user['Quarter'].append(int(data.strip('.json')))
                    except:
                        pass

    df_map_user = pd.DataFrame(Dict_user)
    df_map_user['State'] = df_map_user['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'})
    df_map_user.to_csv('map_user.csv', index=False)
    #print(df_map_user)
    return df_map_user

def m_user(user_df):
    cursor.execute("DROP table IF EXISTS map_user;")
    db.commit()
    cursor.execute("create table map_user (State varchar(100), Year int, Quarter int, District varchar(100), RegisteredUsers int, AppOpens int)")
    for i,row in user_df.iterrows():
        sql = "INSERT INTO map_user VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        db.commit()

#table 5
def top_dist_trans_file(t_path,top_dist_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'District_Name': [], 'Count': [], 'Amount': []}
    for state in top_dist_trans_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        for year in year_list:
            year_path=state_path+year+"/"
            Q_file=os.listdir(year_path)
            #print(Q_file)
            for data in Q_file:
                file_path=year_path+data
                file_content= open(file_path, "r")
                file_data=json.load(file_content)
                #print(file_data)

                for i in  file_data["data"]["districts"]:
                    trans_dist_name=i["entityName"]
                    trans_count=i["metric"]["count"]
                    trans_amount=i["metric"]["amount"]
                    Dict_trans['District_Name'].append(trans_dist_name)
                    Dict_trans['Count'].append(trans_count)
                    Dict_trans['Amount'].append(trans_amount)
                    Dict_trans['State'].append(state)
                    Dict_trans['Year'].append(year)
                    Dict_trans['Quarter'].append(int(data.strip('.json')))


    df_top_dist_trans=pd.DataFrame(Dict_trans)
    df_top_dist_trans['State'] = df_top_dist_trans['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'})
    df_top_dist_trans.to_csv('top_dist_trans.csv', index=False)
    #print(df_top_dist_trans)
    return df_top_dist_trans

def top_dist_trans(top_dist_trans_df):
    cursor.execute("DROP table IF EXISTS top_dist_trans;")
    db.commit()
    cursor.execute("create table top_dist_trans (State varchar(100), Year int, Quarter int, District varchar(100), Transaction_Count int, Transaction_Amount real)")
    for i,row in top_dist_trans_df.iterrows():
        sql = "INSERT INTO top_dist_trans VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        db.commit()


#table 6
def top_pin_trans_file(t_path,top_pin_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'Pincode': [], 'Count': [], 'Amount': []}
    for state in top_pin_trans_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        for year in year_list:
            year_path=state_path+year+"/"
            Q_file=os.listdir(year_path)
            #print(Q_file)
            for data in Q_file:
                file_path=year_path+data
                file_content= open(file_path, "r")
                file_data=json.load(file_content)
                #print(file_data)
                try:
                    for i in  file_data["data"]["pincodes"]:
                        trans_pin_name=i["entityName"]
                        trans_count=i["metric"]["count"]
                        trans_amount=i["metric"]["amount"]
                        Dict_trans['Pincode'].append(trans_pin_name)
                        Dict_trans['Count'].append(trans_count)
                        Dict_trans['Amount'].append(trans_amount)
                        Dict_trans['State'].append(state)
                        Dict_trans['Year'].append(year)
                        Dict_trans['Quarter'].append(int(data.strip('.json')))
                except:
                    pass


    df_top_pin_trans=pd.DataFrame(Dict_trans)
    df_top_pin_trans['State'] = df_top_pin_trans['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'})
    df_top_pin_trans.to_csv('top_pin_trans.csv', index=False)
    return df_top_pin_trans

def top_pin_trans(top_pin_trans_df):
    cursor.execute("DROP table IF EXISTS top_pin_trans;")
    db.commit()
    cursor.execute("create table top_pin_trans (State varchar(100), Year int, Quarter int, Pincode int, Transaction_Count int, Transaction_Amount real)")
    for i,row in top_pin_trans_df.iterrows():
        sql = "INSERT INTO top_pin_trans VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        db.commit()


#table 7
def top_dist_user_file(t_path,top_dist_user_list):
    Dict_user = {'State': [], 'Year': [], 'Quarter': [], 'District_Name': [], 'RegisteredUsers': []}
    for state in top_dist_user_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        for year in year_list:
            year_path=state_path+year+"/"
            Q_file=os.listdir(year_path)
            #print(Q_file)
            for data in Q_file:
                file_path=year_path+data
                file_content= open(file_path, "r")
                file_data=json.load(file_content)
                #print(file_data)

                for i in  file_data["data"]["districts"]:
                    user_dist_name=i["name"]
                    reg_users_count=i["registeredUsers"]
                    Dict_user['District_Name'].append(user_dist_name)
                    Dict_user['RegisteredUsers'].append(reg_users_count)
                    Dict_user['State'].append(state)
                    Dict_user['Year'].append(year)
                    Dict_user['Quarter'].append(int(data.strip('.json')))


    df_top_dist_user=pd.DataFrame(Dict_user)
    df_top_dist_user['State'] = df_top_dist_user['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'})
    df_top_dist_user.to_csv('top_dist_user.csv', index=False)
    #print(df_top_dist_user)
    return df_top_dist_user

def top_dist_user(top_dist_user_df):
    cursor.execute("DROP table IF EXISTS top_dist_user;")
    db.commit()
    cursor.execute("create table top_dist_user (State varchar(100), Year int, Quarter int, District_Name varchar(100), Registered_User int)")
    for i,row in top_dist_user_df.iterrows():
        sql = "INSERT INTO top_dist_user VALUES (%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        db.commit()


#table 8
def top_pin_user_file(t_path,top_pin_user_list):
    Dict_user = {'State': [], 'Year': [], 'Quarter': [], 'pincode': [], 'RegisteredUsers': []}
    for state in top_pin_user_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        for year in year_list:
            year_path=state_path+year+"/"
            Q_file=os.listdir(year_path)
            #print(Q_file)
            for data in Q_file:
                file_path=year_path+data
                file_content= open(file_path, "r")
                file_data=json.load(file_content)
                #print(file_data)

                for i in  file_data["data"]["pincodes"]:
                    user_pin=i["name"]
                    reg_users_count=i["registeredUsers"]
                    Dict_user['pincode'].append(user_pin)
                    Dict_user['RegisteredUsers'].append(reg_users_count)
                    Dict_user['State'].append(state)
                    Dict_user['Year'].append(year)
                    Dict_user['Quarter'].append(int(data.strip('.json')))


    df_top_pin_user=pd.DataFrame(Dict_user)
    df_top_pin_user['State'] = df_top_pin_user['State'].replace(
        {'andaman-&-nicobar-islands': 'Andaman & Nicobar Island', 'andhra-pradesh': 'Andhra Pradesh',
         'arunachal-pradesh': 'Arunanchal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar', 'chandigarh': 'Chandigarh',
         'chhattisgarh': 'Chhattisgarh', 'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
         'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat', 'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
         'jammu-&-kashmir': 'Jammu & Kashmir', 'jharkhand': 'Jharkhand', 'karnataka': 'Karnataka', 'kerala': 'Kerala',
         'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep', 'madhya-pradesh': 'Madhya Pradesh', 'maharashtra': 'Maharashtra',
         'manipur': 'Manipur', 'meghalaya': 'Meghalaya', 'mizoram': 'Mizoram', 'nagaland': 'Nagaland', 'odisha': 'Odisha',
         'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan', 'sikkim': 'Sikkim', 'tamil-nadu': 'Tamil Nadu',
         'telangana': 'Telangana', 'tripura': 'Tripura', 'uttar-pradesh': 'Uttar Pradesh', 'uttarakhand': 'Uttarakhand',
         'west-bengal': 'West Bengal'})
    df_top_pin_user.to_csv('top_dist_user.csv', index=False)
    #print(df_top_pin_user)
    return df_top_pin_user

def top_pin_user(top_pin_user_df):
    cursor.execute("DROP table IF EXISTS top_pin_user;")
    db.commit()
    cursor.execute("create table top_pin_user (State varchar(100), Year int, Quarter int, Pincode varchar(100), Registered_User int)")
    for i,row in top_pin_user_df.iterrows():
        sql = "INSERT INTO top_pin_user VALUES (%s,%s,%s,%s,%s)"
        cursor.execute(sql, tuple(row))
        db.commit()


#1.Aggregated transaction
aggre_trans_path="D:/github_repo/pulse/data/aggregated/transaction/country/india/state/"
aggre_trans_list=os.listdir(aggre_trans_path)
#print(aggri_trans_list)

trans_df= agg_trans_file(aggre_trans_path,aggre_trans_list)
a_trans(trans_df)


#2.Aggregated User
aggre_user_path="D:/github_repo/pulse/data/aggregated/user/country/india/state/"
aggre_user_list=os.listdir(aggre_user_path)

user_df= agg_user_file(aggre_user_path,aggre_user_list)
a_user(user_df)

#3.Map Trans
map_trans_path="D:/github_repo/pulse/data/map/transaction/hover/country/india/state/"
map_trans_list=os.listdir(map_trans_path)

m_trans_df= map_trans_file(map_trans_path,map_trans_list)
m_trans(m_trans_df)

#4.Map User
map_user_path="D:/github_repo/pulse/data/map/user/hover/country/india/state/"
map_user_list=os.listdir(map_user_path)

m_user_df= map_user_file(map_user_path,map_user_list)
m_user(m_user_df)

#5top dist trans
top_dist_trans_path="D:/github_repo/pulse/data/top/transaction/country/india/state/"
top_dist_trans_list=os.listdir(top_dist_trans_path)

top_dist_trans_df= top_dist_trans_file(top_dist_trans_path,top_dist_trans_list)
top_dist_trans(top_dist_trans_df)

#6top pincode trans
top_pin_trans_path="D:/github_repo/pulse/data/top/transaction/country/india/state/"
top_pin_trans_list=os.listdir(top_pin_trans_path)

top_pin_trans_df= top_pin_trans_file(top_pin_trans_path,top_pin_trans_list)
top_pin_trans(top_pin_trans_df)

#7top dist user
top_dist_user_path="D:/github_repo/pulse/data/top/user/country/india/state/"
top_dist_user_list=os.listdir(top_dist_user_path)

top_dist_user_df= top_dist_user_file(top_dist_user_path,top_dist_user_list)
top_dist_user(top_dist_user_df)

#8top pin user
top_pin_user_path="D:/github_repo/pulse/data/top/user/country/india/state/"
top_pin_user_list=os.listdir(top_pin_user_path)

top_pin_user_df= top_pin_user_file(top_pin_user_path,top_pin_user_list)
top_pin_user(top_pin_user_df)