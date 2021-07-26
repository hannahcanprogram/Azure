import pandas as pd

df_1 = pd.read_csv('people/people_1.txt', sep='\t')
df_2 = pd.read_csv('people/people_2.txt', sep='\t')
df = pd.concat([df_1, df_2], ignore_index=True)

df['FirstName'] = df.FirstName.str.lower().str.strip()
df['LastName'] = df.LastName.str.lower().str.strip()
df['Email'] = df.Email.str.lower().str.strip()
df['Phone'] = df.Phone.str.replace('-','').str.strip()
assert all(df.Phone.str.replace('-','').str.isnumeric())

df['Name'] = df.FirstName+df.LastName
df2 = df.drop_duplicates(subset=['Name'],ignore_index=True)
df2 = df2.drop(columns='Name')
df2.to_csv('people_no_duplicate.csv', sep=',', index=False)