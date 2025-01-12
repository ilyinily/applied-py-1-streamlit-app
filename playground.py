import pandas as pd

df = pd.read_csv("./temperature_data.csv")

print(list(df))
for column_name in list(df):
    print(type(df[column_name][0]))

import datetime

failed_string = "01-10-2010"
correct_string = df['timestamp'][0]

right_format = "%Y-%m-%d"

print(bool(datetime.datetime.strptime(correct_string, right_format)))
try:
    print(bool(datetime.datetime.strptime(failed_string, right_format)))
except ValueError:
    print("no match")

print(len(df))

print(df['timestamp'].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$"))

print(df['timestamp'].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$").eq(True).all())

print(list((list(df)[0], list(df)[2], list(df)[3])))

print(df['city'].map(type))

print("*" * 50)

# print(len(df['city'].map(type) != str))
print((df['city'].map(type) == str).eq(True).all())
print(type(df['city'][0]))

print(df[df['city'] == 'Tokyo'][['city', 'temperature']])
print(df[df['city'] == 'Tokyo'][['season', 'temperature']].groupby('season').agg('mean'))