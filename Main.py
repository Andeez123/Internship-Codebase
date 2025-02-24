# standard library
import datetime
import os
import sys

# third-party library
import numpy as np
import pandas as pd

# local file
import Connection as cn

# ----------------------------------------------------------------------------------------------------------------------
pd.set_option('display.max_columns', None)

read_conn = cn.sqlconn()[0]
write_conn = cn.sqlconn()[1]

time_dict = {"start_time": "'1900-01-01 00:00:00'",
             "end_time": "'1900-01-01 23:59:59'"}

# ----------------------------------------------------------------------------------------------------------------------
print(f'Reading From OceanBase: {datetime.datetime.now()}', end='\n\n')
campaign_part = cn.sqlfile('../SQL/Campaign_participant.sql', read_conn, time_dict) #reading sql query, loading the data to python for processing

campaign_users = campaign_part['campaign_user_id']

mult_id = []
all_ids = []
campaign_ids = campaign_part['campaign_id']

mult_name = []
all_names = []
campaign_names = campaign_part['campaign_full_name']

mult_reward = []
all_rewards = []
campaign_rewards = campaign_part['campaign_reward']

event = []
data = pd.DataFrame(event)

for id in campaign_ids:
    mult_id = id.split(',')
    all_ids.append(mult_id)

for name in campaign_names:
    mult_name = name.split(',')
    all_names.append(mult_name)

for reward in campaign_rewards:
    mult_reward = reward.split(',')
    all_rewards.append(mult_reward)

for count, user in enumerate(campaign_users):
    names = all_names[count]
    ids = all_ids[count]
    for id, name in zip(ids, names):
        event = {"User ID": user.strip(), "ID": id.strip(), "Name": name.strip()}
        print(event)
        data = pd.concat([data,pd.DataFrame([event])])

rewards_list = []
for rewards in all_rewards:
    for reward in rewards:
        rewards_list.append(float(reward))

data['Reward'] = rewards_list
data['update_time_utc8'] = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

# Writing to excel file
# with pd.ExcelWriter('Campaign_part.xlsx', engine="openpyxl") as writer:
#     data.to_excel(writer, sheet_name="Campaign Participants", index=False)
#
# print(f"Data written in Campaign_part.xlsx")

print(f"Writing To OceanBase: {datetime.datetime.now()}", end="\n\n")
with write_conn.connect() as conn:
    data.to_sql('mkt_campaign_participant_split', conn, if_exists='replace', index=False, dtype=cn.sqlcol(data), chunksize=10000)


print("Successfully written to database")
print(sys.executable)
