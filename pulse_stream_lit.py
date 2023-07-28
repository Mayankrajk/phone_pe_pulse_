from googleapiclient.discovery import build
import re
import pandas as pd
import numpy as np
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import mysql.connector
import streamlit as st
from streamlit_option_menu import option_menu
import seaborn as sns

import json
import os
import pandas
import pandas as pd
import mysql.connector
import streamlit as st
import plotly.express as px


con = mysql.connector.connect(host="localhost", user="root", password="Ashu@123", database="pulse_project")
print(con)
cursor = con.cursor()




##----------------------------------------------------------------------------##



styled_text = "<p style='font-size: 30px; font-weight: bold;color:blue;'>Phonepe Pulse Data Visualization and " \
              "Exploration:</p>"
st.markdown(styled_text, unsafe_allow_html=True)

with st.sidebar:
    page = ['About', 'Data Exploration', 'State & District wise', "Overall Stats"]
    select_page = st.selectbox('select the page', page)

def format_amount_inr(amount):
    crore_amount = amount / 10000000  # Divide by 10,000,000 to convert to Crore
    rounded_crore_amount = round(crore_amount)  # Round off the Crore amount
    formatted_amount = '₹{:,.0f} Cr'.format(rounded_crore_amount)
    return formatted_amount


if select_page == 'About':
    st.write("The Phonepe pulse Github repository contains a large amount of data related to"
             "various metrics and statistics. The goal is to extract this data and process it to obtain"
             " insights and information that can be visualized in a user-friendly manner.")
    st.write("The solution must be secure, efficient, and user-friendly. The dashboard must be"
             "easily accessible and provide valuable insights and information about the data in the"
             "Phonepe pulse Github repository.")
    st.write("Users will be able to access the dashboard from a web browser and easily navigate"
             "the different visualizations and facts and figures displayed. The dashboard will"
             "provide valuable insights and information about the data in the Phonepe pulse"
             " Github repository, making it a valuable tool for data analysis and decision-making.")

    st.write("Overall, the result of this project will be a comprehensive and user-friendly solution"
             "for extracting, transforming, and visualizing data from the Phonepe pulse Github"
             "repository.")




if select_page == 'Data Exploration':
    with st.sidebar:
        options = ['User', 'Transaction']
        selected_option = st.selectbox('Type', options)
        year = [2018, 2019, 2020, 2021, 2022]
        selected_year = st.selectbox('Select the year', year)
        quarter = ['Q1 (Jan-Mar)', 'Q2 (Apr-Jun)', 'Q3 (July-Sep)', 'Q4 (Oct-Dec)']
        selected_quarter = st.selectbox('Select the quarter', quarter)

    if selected_option == 'Transaction':
        tab1, tab2 = st.tabs(["Total_Transaction_Count", "Total_Transaction_Amount"])
        if selected_year and selected_quarter:
            with tab1:
                col1, col2 = st.columns([2.5, 1])
                with col1:
                    # 1 Transaction user According To year and Quarter Statewise bar plot
                    query1 = f"SELECT state ,sum(count) as count FROM aggre_trans_state_wise WHERE quarter = '{selected_quarter[:2]}' AND year = '{selected_year}' group by state order by count desc"
                    cursor.execute(query1)
                    result1 = cursor.fetchall()
                    df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                    fig = px.bar(df1,
                                 title='Total_Transaction  According To year and Quarter Statewise',
                                 x="state",
                                 y="count",
                                 orientation='v',
                                 color='count',
                                 color_continuous_scale='Blues')
                    st.plotly_chart(fig, use_container_width=True)

                    # 2 Transaction user According To year and Quarter Category wise bar plot
                    query1 = f"SELECT categories,sum(count) as count FROM aggre_trans_state_wise WHERE quarter = '{selected_quarter[:2]}' AND year = '{selected_year}' group by categories order by count desc "
                    cursor.execute(query1)
                    result1 = cursor.fetchall()
                    df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                    fig = px.bar(df1,
                                 title='Total_Transaction  According To year and Quarter Category wise',
                                 x="categories",
                                 y="count",
                                 orientation='v',
                                 color='count',
                                 color_continuous_scale=px.colors.sequential.Magenta)
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.subheader('Transactions :')
                    query1 = f"select sum(count) from aggre_trans_year_wise where quarter= '{selected_quarter[0:2]}' and year='{selected_year}'"
                    cursor.execute(query1)
                    result1 = int(cursor.fetchone()[0])
                    formatted_amount1 = '{:,}'.format(result1)
                    styled_text = "<p style='font-size: 20px; font-weight: bold;'>All PhonePe transactions (UPI + Cards + Wallets)</p>"
                    styled_text += f"<p style='font-size: 40px;font-weight: bold;color: purple;'>{formatted_amount1}</p>"
                    st.markdown(styled_text, unsafe_allow_html=True)
                    col2_a, col2_b = st.columns([1, 1])
                    with col2_a:
                        query2 = f"select sum(amount) from aggre_trans_year_wise where quarter= '{selected_quarter[0:2]}' and year='{selected_year}'"
                        cursor.execute(query2)
                        result2 = int(cursor.fetchone()[0])
                        formatted_result2 = format_amount_inr(result2)
                        styled_text = "<p style='font-size: 16px; font-weight: bold;'>All payment Value</p>"
                        styled_text += f"<p style='font-size: 25px;font-weight: bold;color: purple;'>{formatted_result2}</p>"
                        st.markdown(styled_text, unsafe_allow_html=True)
                    with col2_b:
                        result3 = round(result2 / result1)
                        formatted_result3 = '₹{:,}'.format(result3)
                        styled_text = "<p style='font-size: 16px; font-weight: bold;'>Avg. Trans. Value</p>"
                        styled_text += f"<p style='font-size: 20px;font-weight: bold;color: purple;'>{formatted_result3}</p>"
                        st.markdown(styled_text, unsafe_allow_html=True)

                    st.subheader('Categories :')
                    query = f"select Categories, count from aggre_trans_year_wise where quarter='{selected_quarter[0:2]}' and year='{selected_year}' "
                    cursor.execute(query)
                    result = cursor.fetchall()
                    # Execute each query and fetch the results
                    df = pd.DataFrame(result, columns=['Categories', 'count'])
                    for val in range(len(df['Categories'])):
                        formatted_result = '{:,} '.format(df['count'][val])  # Format the result with commas
                        Categories = df['Categories'][val]
                        styled_text = f"<span style='font-size: 15px; font-weight: bold;'>{Categories}  :          </span> <span style='font-size: 15px; font-weight: bold; color: purple;'>{formatted_result}</span>"
                        st.markdown(styled_text, unsafe_allow_html=True)

                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.subheader("Top 10 Toal_Trasaction_Count ")
                    col2_c, col2_d, col2_e = st.columns([1, 1, 1])

                    with col2_c:
                        b1 = st.button("States")
                    with col2_d:
                        b2 = st.button("Districts")
                    with col2_e:
                        b3 = st.button("Pincode")

                    if b1:
                        st.subheader('Top 10 states')
                        query = f"SELECT state,state_count FROM top_trans_year_wise where quarter = '{selected_quarter[0:2]}' and year = '{selected_year}' "
                        cursor.execute(query)
                        val = cursor.fetchall()
                        df = pd.DataFrame(val, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                          columns=[des[0] for des in cursor.description])
                        st.dataframe(df)
                        with col1:
                            cursor.execute(query)
                            result1 = cursor.fetchall()
                            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                            fig = px.bar(df1,
                                         title='Top 10 states According To year and Quarter',
                                         x="state",
                                         y="state_count",
                                         orientation='v',
                                         color='state_count',
                                         color_continuous_scale=px.colors.sequential.Magenta)
                            st.plotly_chart(fig, use_container_width=True)

                    if b2:
                        st.subheader('Top 10 Districts')
                        query = f"SELECT district,district_count FROM top_trans_year_wise where quarter = '{selected_quarter[0:2]}' and year = '{selected_year}' "
                        cursor.execute(query)
                        val = cursor.fetchall()
                        df = pd.DataFrame(val, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                          columns=[des[0] for des in cursor.description])
                        st.dataframe(df)
                        with col1:
                            cursor.execute(query)
                            result1 = cursor.fetchall()
                            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                            fig = px.bar(df1,
                                         title='Top 10 districts According To year and Quarter',
                                         x="district",
                                         y="district_count",
                                         orientation='v',
                                         color='district_count',
                                         color_continuous_scale=px.colors.sequential.Magenta)
                            st.plotly_chart(fig, use_container_width=True)

                    if b3:
                        st.subheader('Top 10 Pinocdes')
                        query = f"SELECT pincode,pincode_count FROM top_trans_year_wise where quarter = '{selected_quarter[0:2]}' and year = '{selected_year}' "
                        cursor.execute(query)
                        val = cursor.fetchall()
                        df = pd.DataFrame(val, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                                          columns=[des[0] for des in cursor.description])
                        st.dataframe(df)
                        with col1:
                            cursor.execute(query)
                            result1 = cursor.fetchall()
                            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                            fig = px.pie(df1, values='pincode_count', names='pincode',
                                         title='Top 10 districts According To year and Quarter brand wise')
                            fig.update_traces(textposition='outside', textinfo='label+percent')
                            fig.update_layout(showlegend=False)
                            st.plotly_chart(fig, use_container_width=True)

                    with col1:
                        st.write("")
                        st.write("")
                        st.write("")
                        st.write("")

            with tab2:
                col3, col4 = st.columns([2, 1])
                with col3:
                    # 1 Transaction amount According To year and Quarter state wise
                    query1 = f"SELECT  state,sum(amount) as amount FROM aggre_trans_state_wise WHERE quarter = '{selected_quarter[:2]}' AND year = '{selected_year}' group by state order by amount desc"
                    cursor.execute(query1)
                    result1 = cursor.fetchall()
                    df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                    fig = px.bar(df1,
                                 title='Total_Amount According To year and Quarter state wise',
                                 x="state",
                                 y="amount",
                                 orientation='v',
                                 color='amount',
                                 color_continuous_scale=px.colors.sequential.Magenta)
                    st.plotly_chart(fig, use_container_width=True)



                with col4:
                    st.subheader('Transactions :')
                    query1 = f"select sum(count) from aggre_trans_year_wise where quarter= '{selected_quarter[0:2]}' and year='{selected_year}'"
                    cursor.execute(query1)
                    result1 = int(cursor.fetchone()[0])
                    formatted_amount1 = '{:,}'.format(result1)
                    styled_text = "<p style='font-size: 20px; font-weight: bold;'>All PhonePe transactions (UPI + Cards + Wallets)</p>"
                    styled_text += f"<p style='font-size: 40px;font-weight: bold;color: purple;'>{formatted_amount1}</p>"
                    st.markdown(styled_text, unsafe_allow_html=True)
                    col4_a, col4_b = st.columns([1, 1])
                    with col4_a:
                        query2 = f"select sum(amount) from aggre_trans_year_wise where quarter= '{selected_quarter[0:2]}' and year='{selected_year}'"
                        cursor.execute(query2)
                        result2 = int(cursor.fetchone()[0])
                        formatted_result2 = format_amount_inr(result2)
                        styled_text = "<p style='font-size: 20px; font-weight: bold;'>All payment Value</p>"
                        styled_text += f"<p style='font-size: 25px;font-weight: bold;color: purple;'>{formatted_result2}</p>"
                        st.markdown(styled_text, unsafe_allow_html=True)
                    with col4_b:
                        result3 = round(result2 / result1)
                        formatted_result3 = '₹{:,}'.format(result3)
                        styled_text = "<p style='font-size: 20px; font-weight: bold;'>Avg. Trans. Value</p>"
                        styled_text += f"<p style='font-size: 25px;font-weight: bold;color: purple;'>{formatted_result3}</p>"
                        st.markdown(styled_text, unsafe_allow_html=True)

                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")
                    st.write("")


    elif selected_option == 'User':

        st.subheader("Top 10 Total_Registered_User ")
        selected_option = st.selectbox('select_option', ["", "states", "districts", "pincode"])
        if selected_option == 'states':
            st.subheader('Top 10 states')
            query = f"SELECT state,state_registered_user FROM top_user_year_wise where quarter = '{selected_quarter[0:2]}' and year = '{selected_year}' "
            cursor.execute(query)
            val = cursor.fetchall()
            df = pd.DataFrame(val, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                              columns=[des[0] for des in cursor.description])
            st.dataframe(df)
            with col5:
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                cursor.execute(query)
                result1 = cursor.fetchall()
                df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                fig = px.bar(df1,
                             title='Top 10 states According To year and Quarter',
                             x="state",
                             y="state_registered_user",
                             orientation='v',
                             color='state_registered_user',
                             color_continuous_scale=px.colors.sequential.Magenta)
                st.plotly_chart(fig, use_container_width=True)
        if selected_option == 'districts':
            st.subheader('Top 10 Districts')
            query = f"SELECT district,district_registered_users FROM top_user_year_wise where quarter = '{selected_quarter[0:2]}' and year = '{selected_year}' "
            cursor.execute(query)
            val = cursor.fetchall()
            df = pd.DataFrame(val, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                              columns=[des[0] for des in cursor.description])
            st.dataframe(df)
            with col5:
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                cursor.execute(query)
                result1 = cursor.fetchall()
                df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                fig = px.bar(df1,
                             title='Top 10 districts According To year and Quarter',
                             x="district",
                             y="district_registered_users",
                             orientation='v',
                             color='district_registered_users',
                             color_continuous_scale=px.colors.sequential.Magenta)
                st.plotly_chart(fig, use_container_width=True)

        if selected_option == 'pincode':
            st.subheader('Top 10 Pinocdes')
            query = f"SELECT pincode,pincode_registered_users FROM top_user_year_wise where quarter = '{selected_quarter[0:2]}' and year = '{selected_year}' "
            cursor.execute(query)
            val = cursor.fetchall()
            df = pd.DataFrame(val, index=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                              columns=[des[0] for des in cursor.description])
            st.dataframe(df)
            with col5:
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                st.write("")
                cursor.execute(query)
                result1 = cursor.fetchall()
                df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                fig = px.pie(df1, values='pincode_registered_users', names='pincode',
                             title='Top 10 pincodes According To year and Quarter ')
                fig.update_traces(textposition='outside', textinfo='label+percent')
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)


if select_page == 'State & District wise':
    with st.sidebar:
        options = ['User']
        selected_option = st.selectbox('Type', options)
        year = [2018, 2019, 2020, 2021, 2022]
        selected_year = st.selectbox('Select the year', year)

        quarter = ['Q1 (Jan-Mar)', 'Q2 (Apr-Jun)', 'Q3 (July-Sep)', 'Q4 (Oct-Dec)']
        selected_quarter = st.selectbox('Select the quarter', quarter)


    if selected_option == 'User':
        tab1, tab2 = st.tabs(["Total_Register_User", "Total_App_Open_Count"])
        with tab1:
            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('Select the state', state_list)
            col1, col2 = st.columns([2.5, 1])
            with col1:
                query1 = f"SELECT district,registered_user  FROM map_user_state_wise WHERE quarter = '{selected_quarter[:2]}' AND year = '{selected_year}' and state='{selected_state}' order by registered_user desc "
                cursor.execute(query1)
                result1 = cursor.fetchall()
                df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                fig = px.bar(df1,
                             title=f"Total_registered_user  insights of '{selected_state}' district",
                             x="district",
                             y="registered_user",
                             orientation='v',
                             color='registered_user',
                             color_continuous_scale=px.colors.sequential.Magenta)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                st.subheader(f"Total_registered_user insights of '{selected_state}' district")
                df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
                st.dataframe(df1)


if select_page == "Overall Stats":
    with st.sidebar:
        options = ['User', 'Transaction']
        selected_option = st.selectbox('Type', options)
    if selected_option == 'Transaction':
        tab1, tab2 = st.tabs(["Total_Transaction_Count", "Total_Transaction_Amount"])
        with tab1:
            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('select the state', state_list)
            query1 = f"SELECT year,sum(count) as trasaction_count FROM map_trans_state_wise WHERE  state='{selected_state}' group by year order by trasaction_count desc"
            cursor.execute(query1)
            result1 = cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
            fig = px.bar(df1,
                         title=f"Total_Transaction_Count  According To year wise '{selected_state}' ",
                         x="year",
                         y="trasaction_count",
                         orientation='v',
                         color='trasaction_count',
                         color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)

            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('Select the state', state_list)

            query = f"select distinct(district) from map_user_state_wise where state='{selected_state}'"
            cursor.execute(query)
            result = cursor.fetchall()
            district_list = []
            for val in result:
                district_list.append(val[0])
            selected_district = st.selectbox('Select the district', district_list)

            query1 = f"SELECT year,sum(count) as transaction_count  FROM map_trans_state_wise WHERE  state='{selected_state}' and district ='{selected_district}' group by year order by transaction_count desc "
            cursor.execute(query1)
            result1 = cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
            fig = px.bar(df1,
                         title=f"Total_Transaction_Count  According To year wise '{selected_district}' ",
                         x="year",
                         y="transaction_count",
                         orientation='v',
                         color='transaction_count',
                         color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)
        with tab2:
            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('Select The state', state_list)
            query1 = f"SELECT year,sum(amount) as transaction_amount  FROM map_trans_state_wise WHERE  state='{selected_state}' group by year "
            cursor.execute(query1)
            result1 = cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
            fig = px.bar(df1,
                         title=f"Total_Transaction_Amount  According To year wise '{selected_state}' ",
                         x="year",
                         y="transaction_amount",
                         orientation='v',
                         color='transaction_amount',
                         color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)

            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('select The State', state_list)

            query = f"select distinct(district) from map_user_state_wise where state='{selected_state}'"
            cursor.execute(query)
            result = cursor.fetchall()
            district_list = []
            for val in result:
                district_list.append(val[0])
            selected_district = st.selectbox('select The district', district_list)

            query1 = f"SELECT year,sum(Amount) as trasaction_amount  FROM map_trans_state_wise WHERE  state='{selected_state}' and district ='{selected_district}' group by year "
            cursor.execute(query1)
            result1 = cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
            fig = px.bar(df1,
                         title=f"Total_Transaction_Amount  According To year wise '{selected_district}' ",
                         x="year",
                         y="trasaction_amount",
                         orientation='v',
                         color='trasaction_amount',
                         color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)
    if selected_option == 'User':
        tab3, tab4 = st.tabs(["Total_Register_User", "Total_app_open_Count"])
        with tab3:
            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('select the state', state_list)
            query1 = f"SELECT year,sum(registered_user) as registered_user  FROM map_user_state_wise WHERE  state='{selected_state}' group by year"
            cursor.execute(query1)
            result1 = cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
            fig = px.bar(df1,
                         title=f"Total_Register_User  According To year wise '{selected_state}' ",
                         x="year",
                         y="registered_user",
                         orientation='v',
                         color='registered_user',
                         color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)

            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('Select the State', state_list)

            query = f"select distinct(district) from map_user_state_wise where state='{selected_state}'"
            cursor.execute(query)
            result = cursor.fetchall()
            district_list = []
            for val in result:
                district_list.append(val[0])
            selected_district = st.selectbox('select the district', district_list)

            query1 = f"SELECT year,sum(registered_user) as registered_user  FROM map_user_state_wise WHERE  state='{selected_state}' and district ='{selected_district}' group by year "
            cursor.execute(query1)
            result1 = cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
            fig = px.bar(df1,
                         title=f"Total_Register_User  According To year wise '{selected_district}' ",
                         x="year",
                         y="registered_user",
                         orientation='v',
                         color='registered_user',
                         color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)
        with tab4:
            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('Select the state', state_list)
            query1 = f"SELECT year,sum(app_open) as app_open FROM map_user_state_wise WHERE  state='{selected_state}' group by year "
            cursor.execute(query1)
            result1 = cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
            fig = px.bar(df1,
                         title=f"Total_App_Open_Count  According To year wise '{selected_state}' ",
                         x="year",
                         y="app_open",
                         orientation='v',
                         color='app_open',
                         color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)

            query = "select distinct(state) from map_user_state_wise"
            cursor.execute(query)
            result = cursor.fetchall()
            state_list = []
            for val in result:
                state_list.append(val[0])
            selected_state = st.selectbox('select the State', state_list)

            query = f"select distinct(district) from map_user_state_wise where state='{selected_state}'"
            cursor.execute(query)
            result = cursor.fetchall()
            district_list = []
            for val in result:
                district_list.append(val[0])
            selected_district = st.selectbox('select the District', district_list)

            query1 = f"SELECT year,sum(app_open) as app_open  FROM map_user_state_wise WHERE  state='{selected_state}' and district ='{selected_district}' group by year "
            cursor.execute(query1)
            result1 = cursor.fetchall()
            df1 = pd.DataFrame(result1, columns=[des[0] for des in cursor.description])
            fig = px.bar(df1,
                         title=f"Total_App_Open_Count  According To year wise '{selected_district}' ",
                         x="year",
                         y="app_open",
                         orientation='v',
                         color='app_open',
                         color_continuous_scale=px.colors.sequential.Magenta)
            st.plotly_chart(fig, use_container_width=True)