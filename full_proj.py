import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
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



rupee = u'\u20B9'

def yandQ(x):
    lst=[]
    cursor.execute(x)
    my_tuple_list=cursor.fetchall();
    for a_tuple in my_tuple_list:  # iterates through each tuple
        for item in a_tuple:  # iterates through each tuple items
            lst.append(item)
    lst.sort()
    return lst

def Q_sum(x):
    cursor.execute(x)
    my_tuple_list=cursor.fetchall();
    for a_tuple in my_tuple_list:  # iterates through each tuple
        for item in a_tuple:  # iterates through each tuple items
            y=item
    return y


def top_data(q):
    #lst=[]
    cursor.execute(q)
    my_tuple_list = cursor.fetchall()
    #print(my_tuple_list)
    return my_tuple_list


st.set_page_config(page_title="Phone_Pe_Pulse",page_icon=":tada",layout="wide")

new_title = '<p style="font-family:sans-serif; color:#6b3395; font-size: 24px;">PhonePe Pulse</p>'
st.markdown(new_title, unsafe_allow_html=True)
st.markdown(
                '><p style="font-family:sans-serif; font-size: 14px;">PhonePe is a leading online payment app & a UPI enabled initiative that allows users to make payments seamlessly in India.'
                ' Use the app for instant money transfers with UPI, mobile & DTH recharge, & utility bill payments like gas, water & electricity.'
                'And here, We provide the informations about transaction and user details</p>',

                unsafe_allow_html=True)


page_bg_img= """
<style>
[data-testid="stAppViewContainer"]
{
background-color: white;
}  
[data-testid="stHeader"]
{
background-color: #c8c9cc;
}
[data-testid="stSidebar"]
{
background-color: #c8c9cc;
text-decoration-color: red;
}

</style>
"""
st.markdown(page_bg_img, unsafe_allow_html=True)


lst1="SELECT DISTINCT(YEAR) FROM AGG_TRANS;"
year=yandQ(lst1)
#print(year)
#print(type(year[0]))


lst2="SELECT DISTINCT(quarter) FROM AGG_TRANS;"
Quarter=yandQ(lst2)
#print(Quarter)
#print(type(Quarter[0]))
#rint(year,Quarter)

st.sidebar.title("INDIA : PhonePe Pulse")
selected = st.sidebar.selectbox("Choose Your Option",(["Transaction","User"]))
add_year = st.sidebar.selectbox("Choose the year?",(year))
add_quarter = st.sidebar.selectbox("Choose the Quarter?",(Quarter))

#Transaction Part
if selected== "Transaction":
    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown(
                '__<p style="font-family:sans-serif; color:#6b3395; font-size: 20px;">Transaction</p>__',
                unsafe_allow_html=True)


    with col1:
        st.markdown('__<p style="font-family:sans-serif; color:#6b3395; font-size: 14px;">All PhonePe Transaction(UPI + Cards + Vallets)</p>__', unsafe_allow_html=True)
        at_c = f"SELECT sum(transaction_count) FROM AGG_TRANS WHERE YEAR={add_year} AND QUARTER={add_quarter};"
        at_count = Q_sum(at_c)
        st.write(at_count)
        col3, col4 = st.columns([1, 1])
        with col3:
            st.markdown('__<p style="font-family:sans-serif; color:#6b3395; font-size: 14px;">Total Payment value</p>__',
                unsafe_allow_html=True)
            at_a =f"SELECT sum(transaction_amount) FROM AGG_TRANS WHERE YEAR={add_year} AND QUARTER={add_quarter};"
            at_amount = Q_sum(at_a)
            st.write(f"{rupee}", round(at_amount / 10000000), "Cr")
        with col4:
            st.markdown('__<p style="font-family:sans-serif; color:#6b3395; font-size: 14px;">Average Transaction Value</p>__',
                unsafe_allow_html=True)
            avg_t = at_amount / at_count
            st.write(f"{rupee}",round(avg_t))

        st.markdown('--------------------------------------------------------------',unsafe_allow_html=True)
    with col2:
        st.markdown(
            '__<p style="font-family:sans-serif; color:#6b3395; font-size: 20px;">Categories</p>__',
            unsafe_allow_html=True)
        m_pay = f"SELECT sum(transaction_count) FROM AGG_TRANS WHERE TRANSACTION_TYPE= 'Merchant payments' AND YEAR={add_year} AND QUARTER={add_quarter};"
        m_pay = Q_sum(m_pay)
        st.write("Merchant payments",m_pay)
        p2p_pay = f"SELECT sum(transaction_count) FROM AGG_TRANS WHERE TRANSACTION_TYPE= 'Peer-to-peer payments' AND YEAR={add_year} AND QUARTER={add_quarter};"
        p2p_pay = Q_sum(p2p_pay)
        st.write("Peer-to-peer payments    ", p2p_pay)
        rb_pay = f"SELECT sum(transaction_count) FROM AGG_TRANS WHERE TRANSACTION_TYPE= 'Recharge & bill payments' AND YEAR={add_year} AND QUARTER={add_quarter};"
        rb_pay = Q_sum(rb_pay)
        st.write("Recharge & bill payments    ", rb_pay)
        fs = f"SELECT sum(transaction_count) FROM AGG_TRANS WHERE TRANSACTION_TYPE= 'Financial Services' AND YEAR={add_year} AND QUARTER={add_quarter};"
        fs = Q_sum(fs)
        st.write("Financial Services    ", fs)
        others = f"SELECT sum(transaction_count) FROM AGG_TRANS WHERE TRANSACTION_TYPE= 'Others' AND YEAR={add_year} AND QUARTER={add_quarter};"
        others = Q_sum(others)
        st.write("Others    ", m_pay)



    st.markdown(
    '__<p style="font-family:sans-serif; color:#6b3395; font-size: 20px;">Top 10</p>__',
                    unsafe_allow_html=True)
    st.markdown(
    '><p style="font-family:sans-serif; font-size: 15px;">The Top 10 Transactions provides tables and charts that list the 10 transaction with states, districts and pincodes</p>',
    unsafe_allow_html = True)
    c1,c2,c3=  st.columns([1,1,1])
    with c1:
        b1=st.button("STATES")
    with c2:
        b2=st.button("DISTRICTS")
    with c3:
        b3=st.button("PINCODES")
    col_a,col_b=st.columns([1,1])
    with c1:
        if b1:
            with col_a:
                st.markdown(
                    '<p style="font-family:sans-serif; color:#6b3395; font-size: 15px;">Top 10 States</p>',
                    unsafe_allow_html=True)
                s = f"SELECT state,transaction_amount FROM top_dist_trans WHERE YEAR={add_year} AND QUARTER={add_quarter} ORDER BY transaction_amount DESC LIMIT 10;"
                dfs = top_data(s)
                df1 = pd.DataFrame(dfs, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], columns=["STATE", "TRANSACTION AMOUNT"])
                st.dataframe(df1)
            with col_b:
                st.write("")
                fig = px.pie(df1, values='TRANSACTION AMOUNT', names='STATE', hole=.4,
                             title='Top 10 States')
                st.plotly_chart(fig, theme=None, use_container_width=True)

    with c2:
        if b2:
            with col_a:
                st.markdown(
                    '<p style="font-family:sans-serif; color:#6b3395; font-size: 15px;">Top 10 Districts</p>',
                    unsafe_allow_html=True)
                s = f"SELECT district,transaction_amount FROM top_dist_trans WHERE YEAR={add_year} AND QUARTER={add_quarter} ORDER BY transaction_amount DESC LIMIT 10;"
                dfd = top_data(s)
                df2 = pd.DataFrame(dfd, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], columns=["DISTRICT", "TRANSACTION AMOUNT"])
                st.dataframe(df2)
            with col_b:
                st.write("")
                fig = px.pie(df2, values='TRANSACTION AMOUNT', names='DISTRICT', hole=.4,
                             title='Top 10 Districts')
                st.plotly_chart(fig, theme=None, use_container_width=True)

    with c3:
        if b3:
            with col_a:
                st.markdown(
                    '<p style="font-family:sans-serif; color:#6b3395; font-size: 15px;">Top 10 Pincodes</p>',
                    unsafe_allow_html=True)
                s = f"SELECT pincode,transaction_amount FROM top_pin_trans WHERE YEAR={add_year} AND QUARTER={add_quarter} ORDER BY transaction_amount DESC LIMIT 10;"
                dfp = top_data(s)
                df3 = pd.DataFrame(dfp, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], columns=["PINCODE", "TRANSACTION AMOUNT"])
                st.dataframe(df3)
            with col_b:
                st.write("")
                fig = px.pie(df3, values='TRANSACTION AMOUNT', names='PINCODE', hole=.4,
                             title='Top 10 Pincodes')
                st.plotly_chart(fig, theme=None, use_container_width=True)

    with col_a:
        if (not b1) and (not b2) and (not b3):
            with col_a:
                st.markdown(
                    '><p style="font-family:sans-serif; color:#6b3395; font-size: 14px;">'
                    ''
                    'Click above and explore</p>',
                    unsafe_allow_html=True)


    st.markdown('--------------', unsafe_allow_html=True)
    st.markdown(
        '__<p style="font-family:sans-serif; color:#6b3395; font-size: 20px;">Map : Transactions</p>__',
        unsafe_allow_html=True)
    st.markdown(
        f'><p style="font-family:sans-serif; font-size: 14px;">Transaction Details of Quarter{add_quarter} in {add_year}.</p>',
        unsafe_allow_html=True)
    st.markdown(
        f'><p style="font-family:sans-serif; font-size: 14px;">This Geomap used to show the location based data.</p>',
        unsafe_allow_html=True)
    t_map = f"select state, sum(transaction_count), sum(transaction_amount) from map_trans where year={add_year} and quarter={add_quarter} group by state;"
    df = pd.DataFrame(top_data(t_map), columns=["India", "All_Transaction", "Total_Payment_Value"])
    df.index = df.index + 1
    df['Avg_Transaction_Value'] = df.apply(lambda row: row.Total_Payment_Value /
                                                       (row.All_Transaction), axis=1)

    fig = px.choropleth(df,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey="properties.ST_NM",
                        locations="India",
                        color="Total_Payment_Value",
                        hover_data=["India", "All_Transaction", "Avg_Transaction_Value"],
                        projection="robinson",
                        color_continuous_scale="Aggrnyl"

                        )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), width=500, height=500)
    st.plotly_chart(fig, theme=None, use_container_width=True)


#User Part
if selected== "User":
    st.markdown(
            '__<p style="font-family:sans-serif; color:#6b3395; font-size: 20px;">Users</p>__',
            unsafe_allow_html=True)



    st.markdown(f'__<p style="font-family:sans-serif; color:#6b3395; font-size: 14px;">Registered PhonePe users till Q{add_quarter} {add_year}</p>__', unsafe_allow_html=True)
    as_c = f"SELECT sum(transaction_count) FROM agg_user WHERE YEAR <={add_year} AND QUARTER <={add_quarter};"
    as_count_till= Q_sum(as_c)
    st.write(as_count_till/ 10000000, "Cr")

    st.markdown(f'__<p style="font-family:sans-serif; color:#6b3395; font-size: 14px;">PhonePe app opens in Q{add_quarter} {add_year}</p>__', unsafe_allow_html=True)
    as_cnt =f"SELECT sum(transaction_count) FROM agg_user WHERE YEAR={add_year} AND QUARTER={add_quarter};"
    as_count = Q_sum(as_cnt)
    if as_count is None:
        st.write(as_count)
    else:
        st.write(as_count/10000000,"Cr")

    st.markdown('--------------',unsafe_allow_html=True)

    st.markdown(
        '__<p style="font-family:sans-serif; color:#6b3395; font-size: 20px;">Top 10</p>__',
        unsafe_allow_html=True)
    st.markdown(
        '><p style="font-family:sans-serif; font-size: 15px;">The Top 10 users provides tables and charts that list the 10 registered users with states, districts and pincodes</p>',
        unsafe_allow_html=True)
    c1,c2,c3=  st.columns([1,1,1])
    with c1:
        b1=st.button("STATES")
    with c2:
        b2=st.button("DISTRICTS")
    with c3:
        b3=st.button("PINCODES")
    col_a, col_b = st.columns([1, 1])
    with c1:
        if b1:
            with col_a:
                st.markdown(
                    '<p style="font-family:sans-serif; color:#6b3395; font-size: 15px;">Top 10 States</p>',
                    unsafe_allow_html=True)
                s = f"SELECT state,registered_user FROM top_dist_user WHERE YEAR={add_year} AND QUARTER={add_quarter} ORDER BY registered_user DESC LIMIT 10;"
                dfs = top_data(s)
                dfs1 = pd.DataFrame(dfs, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], columns=["STATE", "REGISTERED_USERS"])
                st.dataframe(dfs1)
            with col_b:
                st.write("")
                fig = px.pie(dfs1, values='REGISTERED_USERS', names='STATE', hole=.4,
                             title='Top 10 States')
                st.plotly_chart(fig, theme=None, use_container_width=True)
    with c2:
        if b2:
            with col_a:
                st.markdown(
                    '<p style="font-family:sans-serif; color:#6b3395; font-size: 15px;">Top 10 Districts</p>',
                    unsafe_allow_html=True)
                s = f"SELECT district_name,registered_user FROM top_dist_user WHERE YEAR={add_year} AND QUARTER={add_quarter} ORDER BY registered_user DESC LIMIT 10;"
                dfd = top_data(s)
                dfs2 = pd.DataFrame(dfd, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], columns=["DISTRICT", "REGISTERED_USERS"])
                st.dataframe(dfs2)
            with col_b:
                st.write("")
                fig = px.pie(dfs2, values='REGISTERED_USERS', names='DISTRICT', hole=.4,
                             title='Top 10 Districts')
                st.plotly_chart(fig, theme=None, use_container_width=True)
    with c3:
        if b3:
            with col_a:
                st.markdown(
                    '<p style="font-family:sans-serif; color:#6b3395; font-size: 15px;">Top 10 Pincodes</p>',
                    unsafe_allow_html=True)
                s = f"SELECT pincode,registered_user FROM top_pin_user WHERE YEAR={add_year} AND QUARTER={add_quarter} ORDER BY registered_user DESC LIMIT 10;"
                dfp = top_data(s)
                dfs3 = pd.DataFrame(dfp, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], columns=["PINCODE", "REGISTERED_USERS"])
                st.dataframe(dfs3)
            with col_b:
                st.write("")
                fig = px.pie(dfs3, values='REGISTERED_USERS', names='PINCODE',hole=.4,
                             title='Top 10 Pincodes')
                st.plotly_chart(fig, theme=None, use_container_width=True)
    with col_a:
        if (not b1) and (not b2) and (not b3):
            with col_a:
                st.markdown('><p style="font-family:sans-serif;font-size: 14px;">'
                    'Click above and explore</p>',
                    unsafe_allow_html=True)

    st.markdown('--------------', unsafe_allow_html=True)
    st.markdown(
        '__<p style="font-family:sans-serif; color:#6b3395; font-size: 20px;">Map : Users</p>__',
        unsafe_allow_html=True)
    st.markdown(
        f'><p style="font-family:sans-serif; font-size: 14px;">User Details of Quarter{add_quarter} in {add_year}.</p>',
        unsafe_allow_html=True)
    st.markdown(
        f'><p style="font-family:sans-serif; font-size: 14px;">This Geomap used to show the location based data.</p>',
        unsafe_allow_html=True)
    u_map = f"select state, sum(registeredusers), sum(appopens) from map_user where year={add_year} and quarter={add_quarter} group by state;"
    df = pd.DataFrame(top_data(u_map), columns=["India", "Registered_Users", "App_Opens"])
    df.index = df.index + 1
    fig = px.choropleth(df,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey="properties.ST_NM",
                        locations="India",
                        color="Registered_Users",
                        hover_data=["India", "Registered_Users", "App_Opens"],
                        projection="robinson",
                        color_continuous_scale="Aggrnyl"

                        )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), width=500, height=500)
    st.plotly_chart(fig, theme=None, use_container_width=True)
st.markdown('---------------------------------------------------', unsafe_allow_html=True)
st.markdown(
        '__<p style="font-family:sans-serif; color:#6b3395; font-size: 20px;">Year wise Growth Details</p>__',
        unsafe_allow_html=True)
selected1 = option_menu(None, ["Total Payments", "Total Users", "Payment Types", 'Devices',"Contact Detail"],
    menu_icon=None, default_index=0, orientation="horizontal")
selected1

if selected1== "Total Payments":
    st.write(f"India's total PhonePe transaction growth between year {year[0]} to {year[-1]}")
    t_Trans = f"select year, sum(transaction_amount) from agg_trans group by year ;;"
    t_df = pd.DataFrame(top_data(t_Trans), columns=[ "Year", "Transaction_amount"])
    t_df.index = t_df.index + 1
    fig = px.bar(t_df, x='Year', y='Transaction_amount',
                 hover_data=['Year','Transaction_amount'], color='Transaction_amount',
                 width=1200, height=400,color_continuous_scale="Aggrnyl")
    st.plotly_chart(fig, theme=None, use_container_width=True)

if selected1== "Total Users":
    st.write(f"India's total PhonePe users growth between year {year[0]} to {year[-1]}")
    t_user = f"select state, year, sum(registered_user) from top_dist_user group by state, year ;"
    u_df = pd.DataFrame(top_data(t_user), columns=["State", "Year", "Registered_user"])
    u_df.index = u_df.index + 1
    fig = px.bar(u_df, x='Year', y='Registered_user',
                 hover_data=['Year', 'Registered_user'], color='State',
                  width=1200, height=500, color_continuous_scale="Aggrnyl")
    st.plotly_chart(fig, theme=None, use_container_width=True)

if selected1 == "Payment Types":
    st.write(f"India's payment types growth between year {year[0]} to {year[-1]}")
    pt_user = f"select year, transaction_type,sum(transaction_amount) from agg_trans group by year, transaction_type;"
    u_df = pd.DataFrame(top_data(pt_user), columns=["Year", "Transaction_type", "Transaction_amount"])
    u_df.index = u_df.index + 1
    fig = px.bar(u_df, x='Year', y='Transaction_amount',
                 hover_data=['Year', 'Transaction_amount'], color='Transaction_type',
                 width=1200, height=500, color_continuous_scale="Aggrnyl")
    st.plotly_chart(fig, theme=None, use_container_width=True)

if selected1 == "Devices":
    st.write(f"Top devices used for transaction between year {year[0]} to {year[-1]}")
    d_user = f"select year, brand,sum(transaction_count) from agg_user group by year, brand;"
    d_df = pd.DataFrame(top_data(d_user), columns=["Year", "Brand", "Transaction_count"])
    d_df.index = d_df.index + 1
    fig = px.bar(d_df, x='Year', y='Transaction_count',
                 hover_data=['Year', 'Transaction_count'], color='Brand',
                 width=1200, height=500, color_continuous_scale="Aggrnyl")
    st.plotly_chart(fig, theme=None, use_container_width=True)

if selected1 == "Contact Detail":
    st.write(">>PhonePe project:")
    st.write(">>Created by: Priyadharshika.M")
    st.write(">>Linkedin page: https://www.linkedin.com/in/priyadharshika-m-176204269/")
    st.write(">>Github Page: https://github.com/Priyadharshika19")
    st.write(">>Github repository: https://github.com/Priyadharshika19/PhonepePulse_Data_Visualization")












