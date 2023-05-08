PhonePe-Pulse-Data-2018-2022-Analysis
I have created a dashboard to visualize Phonepe pulse Github repository data(https://github.com/PhonePe/pulse#readme) using Streamlit and Plotly in Python


The main contents are:

1 Extracting data from the Phonepe pulse Github repository and
cloning it
![git bash](https://user-images.githubusercontent.com/129640468/236680408-34dda6e5-f95c-4775-9f81-b94edc0d0881.PNG)


2 Transforming json data into table form and storing in database

#table 1
def agg_trans_file(t_path,agg_trans_list):
    Dict_trans = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}
    for state in agg_trans_list:
        state_path=t_path+state+"/"
        year_list=os.listdir(state_path)
        #print(year_list)
        
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
    df_agg_trans.to_csv('agg_trans.csv', index=False) #for reference
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


3 Visualizing the following data  with the help of Plotly and Streamlit.

        * Aggrigated transaction   
        * Aggrigated user
        * Geo-visualization of transaction
        * Geo-visualization of user
        * Top 10 districts according to transaction
        * Top 10 pincodes according to transaction
        * Top 10 districts according to user
        * Top 10 pincodes according to user

