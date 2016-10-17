import pandas as pd

# calculate combined rank for zip codes from demographic data 
# and political donations


def tech_occupations(s):
    keywords = ['TECH', 'SOFTWARE', 'MEDIA', 'INFORMATION']
    for word in keywords:
        if word in s.Occupation:
            return True

    return False


def target_females(s):
    Population = int(s.Population.replace(',', ''))
    return Population * s.FM_Ratio / (1+s.FM_Ratio) * s.Pub_Trans * s.I200K / 1e4

# zip donation 
df = pd.read_csv('indivs_NewYork16.csv', encoding='iso-8859-1')
df = df[pd.notnull(df['Occupation'])]
df['Zip'] = df['Zip'].map(lambda x: str(x))
df['tech_job'] = df.apply(tech_occupations, axis=1)

female_df = df[df['Gender']=='F']
donation_df = female_df[['Zip', 'Amount']].dropna()
donation_by_zip = donation_df.groupby('Zip').sum().sort_values(by='Amount', ascending=False)

female_tech_df = female_df[female_df['tech_job']]
tech_donation_df = female_tech_df[['Zip', 'Amount']].dropna()
tech_donation_by_zip = tech_donation_df.groupby('Zip').sum().sort_values(by='Amount', ascending=False)

# ZipCode demographic 
Manhattan = pd.read_csv('ZipManhattan.csv')
Brooklyn = pd.read_csv('ZipBrooklyn.csv')

Zip = pd.concat([Manhattan, Brooklyn], ignore_index=True)
Zip['Zip'] = Zip['Zip'].map(lambda x: str(x))
Zip['Population'] = Zip['Population'].map(lambda x: int(x))

Zip['targets'] = Zip.apply(target_females, axis=1)
Zip_df['targets'] = Zip_df['targets'].map(lambda x: int(x))

Zip_df = pd.merge(Zip, tech_donation_by_zip, on = 'Zip', how = 'left')
Zip_df['DA'] = Zip_df['targets'].rank(ascending=False)
Zip_df['DW'] = Zip_df['Amount'].rank(ascending=False)
Zip_df['Rank'] = (Zip_df['DA'] + Zip_df['DW']).rank(ascending=False)

Zip_df.to_csv('Zip.csv', index=False)




