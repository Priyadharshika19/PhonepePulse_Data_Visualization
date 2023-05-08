#import following packages
import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
import requests
import plotly.express as px
from streamlit_lottie import st_lottie
import pandas as pd
import os
import json
import plotly.graph_objects as go
import psycopg2


#Create database connection
db=psycopg2.connect(host='localhost', user='postgres', password=******, port=5432, database="PhonePe")
cursor=db.cursor()


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
    print(my_tuple_list)
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












