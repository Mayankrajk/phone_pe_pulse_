import json
import os
import pandas
import pandas as pd
import mysql.connector


def transaction_data():
    aggre_trans_path_year = "C:/Users/mkolwal/PycharmProjects/temp/data/aggregated/transaction/country/india/"
    aggre_trans_list = os.listdir(aggre_trans_path_year)

    aggre_trans_year_wise = dict(categories=[], year=[], quarter=[], count=[], amount=[])
    for year in range(len(aggre_trans_list)):
        json_path = aggre_trans_path_year + aggre_trans_list[year] + "/"
        json_file_list = os.listdir(json_path)
        quarter = ["Q1", "Q2", "Q3", "Q4"]
        for json_file in range(len(json_file_list)):
            with open(f"{json_path + json_file_list[json_file]}") as df:
                file = json.load(df)
            aggre_trans_year_wise['categories'].append(file['data']['transactionData'][0]['name'])
            aggre_trans_year_wise['year'].append(aggre_trans_list[year])
            aggre_trans_year_wise['quarter'].append(quarter[json_file])
            aggre_trans_year_wise['count'].append(file['data']['transactionData'][0]['paymentInstruments'][0]['count'])
            aggre_trans_year_wise['amount'].append(
                file['data']['transactionData'][0]['paymentInstruments'][0]['amount'])

    aggre_trans_path_state = "C:/Users/mkolwal/PycharmProjects/temp/data/aggregated/transaction/country/state"
    aggre_trans_state_wise = dict(categories=[], year=[], quarter=[], state=[], count=[], amount=[])
    state_path = aggre_trans_path_state + "/"
    state_list = os.listdir(state_path)

    for state in range(len(state_list)):
        year_path = state_path + state_list[state] + "/"
        years_list = os.listdir(year_path)

        for year in range(len(years_list)):
            json_path = year_path + "/" + years_list[year]
            json_file_list = os.listdir(json_path)
            for json_file in range(len(json_file_list)):
                df = open(f"{json_path + '/' + json_file_list[json_file]}")
                file = json.load(df)

                aggre_trans_state_wise['categories'].append(file['data']['transactionData'][0]['name'])
                aggre_trans_state_wise['year'].append(years_list[year])
                aggre_trans_state_wise['state'].append(state_list[state])
                aggre_trans_state_wise['quarter'].append(quarter[json_file])
                aggre_trans_state_wise['count'].append(
                    file['data']['transactionData'][0]['paymentInstruments'][0]['count'])
                aggre_trans_state_wise['amount'].append(
                    file['data']['transactionData'][0]['paymentInstruments'][0]['amount'])

    aggre_user_year = "C:/Users/mkolwal/PycharmProjects/temp/data/aggregated/user/country/india/"
    aggre_user_list = os.listdir(aggre_user_year)

    aggre_user_year_wise = dict(registered_user=[], app_open=[], year=[], quarter=[], brand=[], count=[], percentage=[])

    for year in range(len(aggre_user_list) - 1):
        json_path = aggre_user_year + aggre_user_list[year] + "/"
        json_file_list = os.listdir(json_path)
        for json_file in range(len(json_file_list)):
            df = open(f"{json_path + json_file_list[json_file]}")
            file = json.load(df)
            quarter = ["Q1", "Q2", "Q3", "Q4"]
            usersByDevice_list = file['data']['usersByDevice']
            if usersByDevice_list is not None:
                for userBydevice in range(len(usersByDevice_list)):
                    aggre_user_year_wise['registered_user'].append(file['data']['aggregated']['registeredUsers'])
                    aggre_user_year_wise['app_open'].append(file['data']['aggregated']['appOpens'])
                    aggre_user_year_wise['year'].append(aggre_user_list[year])
                    aggre_user_year_wise['quarter'].append(quarter[json_file])
                    aggre_user_year_wise['brand'].append(usersByDevice_list[userBydevice]['brand'])
                    aggre_user_year_wise['count'].append(usersByDevice_list[userBydevice]['count'])
                    aggre_user_year_wise['percentage'].append(usersByDevice_list[userBydevice]['percentage'])
            else:
                aggre_user_year_wise['registered_user'].append(file['data']['aggregated']['registeredUsers'])
                aggre_user_year_wise['app_open'].append(file['data']['aggregated']['appOpens'])
                aggre_user_year_wise['year'].append(aggre_user_list[year])
                aggre_user_year_wise['quarter'].append(quarter[json_file])
                aggre_user_year_wise['brand'].append(None)
                aggre_user_year_wise['count'].append(0)
                aggre_user_year_wise['percentage'].append(None)

    aggre_user_state = "C:/Users/mkolwal/PycharmProjects/temp/data/aggregated/user/country/state"
    aggre_user_list = os.listdir(aggre_user_state)

    aggre_user_state_wise = dict(registered_user=[], app_open=[], year=[], quarter=[], state=[], brand=[], count=[],
                                 percentage=[])

    state_path = aggre_user_state + "/"
    state_list = os.listdir(state_path)
    for state in state_list:
        year_path = state_path + state + "/"
        year_list = os.listdir(year_path)
        for year in range(len(year_list)):
            json_path = year_path + year_list[year] + "/"
            json_list = os.listdir(json_path)
            for json_file in range(len(json_list)):
                df = open(f"{json_path + json_list[json_file]}")
                file = json.load(df)
            quarter = ["Q1", "Q2", "Q3", "Q4"]
            usersByDevice_list = file['data']['usersByDevice']
            if usersByDevice_list is not None:
                for userBydevice in range(len(usersByDevice_list)):
                    aggre_user_state_wise['registered_user'].append(file['data']['aggregated']['registeredUsers'])
                    aggre_user_state_wise['app_open'].append(file['data']['aggregated']['appOpens'])
                    aggre_user_state_wise['year'].append(year_list[year])
                    aggre_user_state_wise['quarter'].append(quarter[json_file])
                    aggre_user_state_wise['state'].append(state)
                    aggre_user_state_wise['brand'].append(usersByDevice_list[userBydevice]['brand'])
                    aggre_user_state_wise['count'].append(usersByDevice_list[userBydevice]['count'])
                    aggre_user_state_wise['percentage'].append(usersByDevice_list[userBydevice]['percentage'])
            else:
                aggre_user_state_wise['registered_user'].append(file['data']['aggregated']['registeredUsers'])
                aggre_user_state_wise['app_open'].append(file['data']['aggregated']['appOpens'])
                aggre_user_state_wise['year'].append(aggre_user_list[year])
                aggre_user_state_wise['quarter'].append(quarter[json_file])
                aggre_user_state_wise['state'].append(state)
                aggre_user_state_wise['brand'].append(None)
                aggre_user_state_wise['count'].append(0)
                aggre_user_state_wise['percentage'].append(None)

    aggre_trans_year_wise_df = pd.DataFrame(aggre_trans_year_wise)
    aggre_trans_state_wise_df = pd.DataFrame(aggre_trans_state_wise)
    aggre_user_year_wise_df = pd.DataFrame(aggre_user_year_wise)

    aggre_user_state_wise_df = pd.DataFrame(aggre_user_state_wise)
    aggre_user_state_wise_df = aggre_user_state_wise_df.fillna(value=0)

    df1 = [aggre_trans_year_wise_df, aggre_trans_state_wise_df, aggre_user_year_wise_df, aggre_user_state_wise_df]
    return df1

#print(transaction_data()[3])


def map_data():
    map_transaction_path = "C:/Users/mkolwal/PycharmProjects/temp/data/map/transaction/hover/country/india/"
    map_transaction_list = os.listdir(map_transaction_path)

    map_transaction_year_wise = dict(categories=[], year=[], quarter=[], count=[], amount=[])

    for year in range(len(map_transaction_list) - 1):
        json_path = map_transaction_path + map_transaction_list[year] + "/"
        json_file_list = os.listdir(json_path)
        for json_file in range(len(json_file_list)):
            df = open(f"{json_path + json_file_list[json_file]}")
            file = json.load(df)
            hoverDataList = file['data']['hoverDataList']
            quarter = ["Q1", "Q2", "Q3", "Q4"]
            for values in hoverDataList:
                map_transaction_year_wise['categories'].append(values['name'])
                map_transaction_year_wise['year'].append(map_transaction_list[year])
                map_transaction_year_wise['quarter'].append(quarter[json_file])
                map_transaction_year_wise['count'].append(values['metric'][0]['count'])
                map_transaction_year_wise['amount'].append(values['metric'][0]['amount'])

    map_transaction_path = "C:/Users/mkolwal/PycharmProjects/temp/data/map/transaction/hover/country/state/"
    map_transaction_list = os.listdir(map_transaction_path)

    map_transaction_state_wise = dict(district=[], year=[], quarter=[], state=[], count=[], amount=[])
    state_path = map_transaction_path + "/"
    state_list = os.listdir(state_path)
    for state in state_list:
        year_path = state_path + state + "/"
        year_list = os.listdir(year_path)
        for year in year_list:
            json_path = year_path + year + "/"
            json_file_list = os.listdir(json_path)
            for json_file in range(len(json_file_list)):
                df = open(f"{json_path + json_file_list[json_file]}")
                file = json.load(df)
            hoverDataList = file['data']['hoverDataList']
            quarter = ["Q1", "Q2", "Q3", "Q4"]
            for values in hoverDataList:
                map_transaction_state_wise['district'].append(values['name'])
                map_transaction_state_wise['year'].append(year)
                map_transaction_state_wise['quarter'].append(quarter[json_file])
                map_transaction_state_wise['state'].append(state)
                map_transaction_state_wise['count'].append(values['metric'][0]['count'])
                map_transaction_state_wise['amount'].append(values['metric'][0]['amount'])

    map_user_path = "C:/Users/mkolwal/PycharmProjects/temp/data/map/user/hover/country/india/"
    map_user_list = os.listdir(map_user_path)

    map_user_year_wise = dict(registered_users=[], app_open=[], state=[], year=[], quarter=[])

    for year in range(len(map_user_list)):
        json_path = map_user_path + map_user_list[year] + "/"
        json_file_list = os.listdir(json_path)
        for json_file in range(len(json_file_list)):
            df = open(f"{json_path + json_file_list[json_file]}")
            file = json.load(df)
            quarter = ["Q1", "Q2", "Q3", "Q4"]
            state_list = ['puducherry', 'tamil nadu', 'uttar pradesh', 'madhya pradesh', 'andhra pradesh', 'tripura',
                          'lakshadweep', 'manipur', 'maharashtra', 'dadra & nagar haveli & daman & diu', 'meghalaya',
                          'andaman & nicobar islands', 'haryana', 'rajasthan', 'ladakh', 'punjab', 'assam', 'jharkhand',
                          'odisha', 'bihar', 'kerala', 'karnataka', 'chandigarh', 'telangana', 'himachal pradesh',
                          'west bengal', 'gujarat', 'sikkim', 'nagaland', 'mizoram', 'chhattisgarh', 'jammu & kashmir',
                          'goa',
                          'arunachal pradesh', 'delhi', 'uttarakhand']
            for state in state_list:
                map_user_year_wise['registered_users'].append(file['data']['hoverData'][state]['registeredUsers'])
                map_user_year_wise['app_open'].append(file['data']['hoverData'][state]['appOpens'])
                map_user_year_wise['year'].append(map_user_list[year])
                map_user_year_wise['state'].append(state)
                map_user_year_wise['quarter'].append(quarter[json_file])

    map_user_path = "C:/Users/mkolwal/PycharmProjects/temp/data/map/user/hover/country/state"
    map_transaction_list = os.listdir(map_user_path)

    map_user_state_wise = dict(registered_users=[], app_open=[], state=[], district=[], year=[], quarter=[])

    state_path = map_user_path + "/"
    state_list = os.listdir(state_path)
    for state in state_list:
        year_path = state_path + state + "/"
        year_list = os.listdir(year_path)
        for year in year_list:
            json_path = year_path + year + "/"
            json_file_list = os.listdir(json_path)
            for json_file in range(len(json_file_list)):
                df = open(f"{json_path + json_file_list[json_file]}")
                file = json.load(df)
                data = list(file['data']['hoverData'].keys())
                quarter = ["Q1", "Q2", "Q3", "Q4"]
                for values in data:
                    map_user_state_wise['state'].append(state)
                    map_user_state_wise['district'].append(values)
                    map_user_state_wise['year'].append(year)
                    map_user_state_wise['quarter'].append(quarter[json_file])
                    map_user_state_wise['registered_users'].append(file['data']['hoverData'][values]['registeredUsers'])
                    map_user_state_wise['app_open'].append(file['data']['hoverData'][values]['appOpens'])

    map_transaction_year_wise_df = pd.DataFrame(map_transaction_year_wise)
    map_transaction_state_wise_df = pd.DataFrame(map_transaction_state_wise)
    map_user_year_wise_df = pd.DataFrame(map_user_year_wise)
    map_user_state_wise_df = pd.DataFrame(map_user_state_wise)
    df1 = [map_transaction_year_wise_df, map_transaction_state_wise_df, map_user_year_wise_df, map_user_state_wise_df]
    return df1


# print(map_data()[2].columns)


def top_data():
    top_transaction_path = "C:/Users/mkolwal/PycharmProjects/temp/data/top/transaction/country/india/"
    top_transaction_list = os.listdir(top_transaction_path)

    top_transaction_year_wise = dict(state=[], state_count=[], state_amount=[], year=[], quarter=[],
                                     pincode=[], pincode_amount=[], pincode_count=[],
                                     district=[], district_amount=[], district_count=[])

    for year in range(len(top_transaction_list) - 1):
        json_path = top_transaction_path + top_transaction_list[year] + "/"
        json_file_list = os.listdir(json_path)
        for json_file in range(len(json_file_list)):
            df = open(f"{json_path + json_file_list[json_file]}")
            file = json.load(df)
            values1 = file['data']['states']
            values2 = file['data']['districts']
            values3 = file['data']['pincodes']
            quarter = ["Q1", "Q2", "Q3", "Q4"]
            if len(values1) == len(values2) == len(values3):
                for values in range(len(values1)):
                    top_transaction_year_wise['state'].append(values1[values]['entityName'])
                    top_transaction_year_wise['state_count'].append(values1[values]['metric']['count'])
                    top_transaction_year_wise['state_amount'].append(values1[values]['metric']['amount'])
                    top_transaction_year_wise['year'].append(top_transaction_list[year])
                    top_transaction_year_wise['quarter'].append(quarter[json_file])
                    top_transaction_year_wise['district'].append(values2[values]['entityName'])
                    top_transaction_year_wise['district_count'].append(values2[values]['metric']['count'])
                    top_transaction_year_wise['district_amount'].append(values2[values]['metric']['amount'])
                    top_transaction_year_wise['pincode'].append(values3[values]['entityName'])
                    top_transaction_year_wise['pincode_count'].append(values3[values]['metric']['count'])
                    top_transaction_year_wise['pincode_amount'].append(values3[values]['metric']['amount'])

    top_transaction_path = "C:/Users/mkolwal/PycharmProjects/temp/data/top/transaction/country/state/"
    top_transaction_list = os.listdir(top_transaction_path)

    state_path = top_transaction_path + "/"
    state_list = os.listdir(state_path)
    top_transaction_state_wise = dict(state=[], district=[], district_count=[], district_amount=[],
                                      year=[], quarter=[], pincode=[], pincode_count=[], pincode_amount=[])
    for state in state_list:
        year_path = state_path + state + "/"
        year_list = os.listdir(year_path)
        for year in year_list:
            json_path = year_path + year + "/"
            json_file_list = os.listdir(json_path)
            for json_file in range(len(json_file_list)):
                df = open(f"{json_path + json_file_list[json_file]}")
                file = json.load(df)
                districts = file['data']['districts']
                quarter = ["Q1", "Q2", "Q3", "Q4"]
                values1 = file['data']['districts']
                values2 = file['data']['pincodes']
                for values in range(len(values2)):
                    try:
                        top_transaction_state_wise['district'].append(values1[values]['entityName'])
                        top_transaction_state_wise['pincode'].append(values2[values]['entityName'])
                        top_transaction_state_wise['state'].append(state)
                        top_transaction_state_wise['pincode_count'].append(values2[values]['metric']['count'])
                        top_transaction_state_wise['pincode_amount'].append(values2[values]['metric']['count'])
                        top_transaction_state_wise['district_count'].append(values1[values]['metric']['count'])
                        top_transaction_state_wise['district_amount'].append(values1[values]['metric']['amount'])
                        top_transaction_state_wise['year'].append(year)
                        top_transaction_state_wise['quarter'].append(quarter[json_file])

                    except IndexError:
                        top_transaction_state_wise['district'].append(None)
                        top_transaction_state_wise['pincode'].append(values2[values]['entityName'])
                        top_transaction_state_wise['state'].append(state)
                        top_transaction_state_wise['pincode_count'].append(values2[values]['metric']['count'])
                        top_transaction_state_wise['pincode_amount'].append(values2[values]['metric']['count'])
                        top_transaction_state_wise['district_count'].append(0)
                        top_transaction_state_wise['district_amount'].append(0)
                        top_transaction_state_wise['year'].append(year)
                        top_transaction_state_wise['quarter'].append(quarter[json_file])

    top_user_path = "C:/Users/mkolwal/PycharmProjects/temp/data/top/user/country/india/"
    top_user_list = os.listdir(top_user_path)

    top_user_year_wise = dict(state=[], state_registered_users=[], year=[], quarter=[],
                              district=[], district_registered_users=[],
                              pincode=[], pincode_registered_users=[])

    for year in range(len(top_user_list) - 1):
        json_path = top_user_path + top_user_list[year] + "/"
        json_file_list = os.listdir(json_path)
        for json_file in range(len(json_file_list)):
            df = open(f"{json_path + json_file_list[json_file]}")
            file = json.load(df)
            states = file['data']['states']
            quarter = ["Q1", "Q2", "Q3", "Q4"]
            values1 = file['data']['states']
            values2 = file['data']['districts']
            values3 = file['data']['pincodes']
            for values in range(len(values1)):
                top_user_year_wise['state'].append(values1[values]['name'])
                top_user_year_wise['state_registered_users'].append(values1[values]['registeredUsers'])
                top_user_year_wise['year'].append(top_user_list[year])
                top_user_year_wise['quarter'].append(quarter[json_file])
                top_user_year_wise['district'].append(values2[values]['name'])
                top_user_year_wise['district_registered_users'].append(values2[values]['registeredUsers'])
                top_user_year_wise['pincode'].append(values3[values]['name'])
                top_user_year_wise['pincode_registered_users'].append(values3[values]['registeredUsers'])

    top_user_path = "C:/Users/mkolwal/PycharmProjects/temp/data/top/user/country/state"
    top_user_list = os.listdir(top_user_path)

    state_path = top_user_path + "/"
    state_list = os.listdir(state_path)
    top_user_state_wise = dict(state=[], district=[], district_registered_users=[], pincode=[],
                               pincode_registered_users=[], year=[], quarter=[])
    for state in state_list:
        year_path = state_path + state + "/"
        year_list = os.listdir(year_path)
        for year in year_list:
            json_path = year_path + year + "/"
            json_file_list = os.listdir(json_path)
            for json_file in range(len(json_file_list)):
                df = open(f"{json_path + json_file_list[json_file]}")
                file = json.load(df)
                # pprint.pprint(file)
                ditrict = file['data']['districts']
                quarter = ["Q1", "Q2", "Q3", "Q4"]
                values1 = file['data']['districts']
                values2 = file['data']['pincodes']
                for values in range(len(values2)):
                    try:
                        top_user_state_wise['district'].append(values1[values]['name'])
                        top_user_state_wise['pincode'].append(values2[values]['name'])
                        top_user_state_wise['state'].append(state)
                        top_user_state_wise['district_registered_users'].append(values1[values]['registeredUsers'])
                        top_user_state_wise['pincode_registered_users'].append(values2[values]['registeredUsers'])
                        top_user_state_wise['year'].append(year)
                        top_user_state_wise['quarter'].append(quarter[json_file])
                    except IndexError:
                        top_user_state_wise['state'].append(state)
                        top_user_state_wise['district'].append(None)
                        top_user_state_wise['pincode'].append(values2[values]['name'])
                        top_user_state_wise['district_registered_users'].append(0)
                        top_user_state_wise['pincode_registered_users'].append(values2[values]['registeredUsers'])
                        top_user_state_wise['year'].append(year)
                        top_user_state_wise['quarter'].append(quarter[json_file])

    top_transaction_year_wise_df = pd.DataFrame(top_transaction_year_wise)
    top_transaction_state_wise_df = pd.DataFrame(top_transaction_state_wise)
    top_user_year_wise_df = pd.DataFrame(top_user_year_wise)
    top_user_state_wise_df = pd.DataFrame(top_user_state_wise)
    df2 = [top_transaction_year_wise_df, top_transaction_state_wise_df, top_user_year_wise_df, top_user_state_wise_df]
    return df2

#print(top_data()[0].columns)




con = mysql.connector.connect(host="localhost", user="root", password="Ashu@123", database="pulse_project")
print(con)
cursor = con.cursor()

# aggre_trans_year_wise
'''for row in transaction_data()[0].itertuples(index=False):
    insert_query = "INSERT INTO aggre_trans_year_wise (categories, year, quarter,count, amount) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()
'''

# aggre_trans_state_wise
'''for row in transaction_data()[1].itertuples(index=False):
    insert_query = "INSERT INTO aggre_trans_state_wise (categories, year, quarter,state, count, amount) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()
'''

# aggre_user_year_wise
'''for row in transaction_data()[2].itertuples(index=False):
    insert_query = "INSERT INTO aggre_user_year_wise (registered_user, app_open, year,quarter, brand, count,percentage ) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()'''


# aggre_user_state_wise
'''for row in transaction_data()[3].itertuples(index=False):
    insert_query = "INSERT INTO aggre_user_state_wise (registered_user, app_open, year,quarter, state, brand, count, percentage ) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s, %s, %s)"
    cursor.execute(insert_query, row)
    # Commit the changes to the database
con.commit()
'''

# map_transaction_year_wise
'''for row in map_data()[0].itertuples(index=False):
    insert_query = "INSERT INTO map_trans_year_wise (categories, year, quarter,count, amount) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()
'''

# map_transaction_state_wise
'''for row in map_data()[1].itertuples(index=False):
    insert_query = "INSERT INTO map_trans_state_wise (district, year, quarter,state,count, amount) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()'''


# map_user_year_wise
'''for row in map_data()[2].itertuples(index=False):
    insert_query = "INSERT INTO map_user_year_wise (registered_user, app_open, state,year, quarter) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()'''

# map_user_state_wise
'''for row in map_data()[3].itertuples(index=False):
    insert_query = "INSERT INTO map_user_state_wise (registered_user, app_open, state,district, year, quarter) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()'''


# top_transaction_year_wise
'''for row in top_data()[0].itertuples(index=False):
    insert_query = "INSERT INTO top_trans_year_wise (state, state_count, state_amount,year, quarter, pincode, pincode_amount, pincode_count, district, district_amount, district_count) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()
'''

# top_transaction_state_wise
'''for row in top_data()[1].itertuples(index=False):
    insert_query = "INSERT INTO top_trans_state_wise (state, district, district_count,district_amount, year, quarter, pincode, pincode_count, pincode_amount) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s, %s, %s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()

'''
# top_user_year_wise
'''for row in top_data()[2].itertuples(index=False):
    insert_query = "INSERT INTO top_user_year_wise (state, state_registered_user, year,quarter, district, district_registered_users,pincode, pincode_registered_users ) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s, %s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()
'''

# top_user_state_wise
'''for row in top_data()[3].itertuples(index=False):
    insert_query = "INSERT INTO top_user_state_wise (state, district, district_registered_user,pincode, pincode_registered_users, year, quarter) VALUES (%s, " \
                   "%s, %s, " \
                   "%s, %s, %s, %s)"
    cursor.execute(insert_query, row)
# Commit the changes to the database
con.commit()'''


print("It's Done Boss!!")



'''aggre_trans_year_wise_csv = transaction_data()[0].to_csv("aggre_trans_year_wise_csv.csv", index=False)
aggre_trans_state_wise_csv = transaction_data()[1].to_csv("aggre_trans_state_wise_csv.csv", index=False)
aggre_user_year_wise_csv = transaction_data()[2].to_csv("aggre_user_year_wise_csv.csv", index=False)
aggre_user_state_wise_csv = transaction_data()[3].to_csv("aggre_user_state_wise_csv.csv", index=False)'''

'''map_transaction_year_wise_csv = map_data()[0].to_csv("map_transaction_year_wise_csv.csv", index=False)
map_transaction_state_wise_csv = map_data()[1].to_csv("map_transaction_state_wise_csv.csv", index=False)
map_user_year_wise_csv = map_data()[2].to_csv("map_user_year_wise_csv.csv", index=False)
map_user_state_wise_csv = map_data()[3].to_csv("map_user_state_wise_csv.csv", index=False)
'''

'''top_transaction_year_wise_csv = top_data()[0].to_csv("top_transaction_year_wise_csv.csv", index=False)
top_transaction_state_wise_csv =  top_data()[1].to_csv("top_transaction_state_wise_csv.csv", index=False)
top_user_year_wise_csv = top_data()[2].to_csv("top_user_year_wise_csv.csv", index=False)
top_user_state_wise_csv = top_data()[3].to_csv("top_user_state_wise_csv.csv", index=False)'''

