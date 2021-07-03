import pandas as pd
import numpy as np
import os
import geopy
from tqdm import tqdm
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from pandas.core.common import SettingWithCopyWarning
import warnings

current_dir = os.path.dirname(os.path.realpath(__file__))
geolocator = geopy.Nominatim(user_agent='myapp_lu')
date_column = ['last_scraped', 'host_since', 'calendar_last_scraped', 'first_review', 'last_review']
str_column = ['license', 'zipcode']

# TODO create DA-master directory under the current directory and move data into it
# if not os.path.exists(os.path.join(current_dir, 'DA-master')):
#     os.mkdir(os.path.join(current_dir, 'DA-master'))


def convert_currency_to_float(x):
    if pd.notnull(x):
        return x.replace('$', '').replace(',', '')
    else:
        return None


def convert_string_to_date(x):
    if pd.notnull(x):
        try:
            date = pd.to_datetime(x, format='%Y/%m/%d').date()
        except ValueError:
            date = pd.to_datetime(x, format='%m/%d/%y', errors='coerce').date()
        if date:
            return date
        else:
            return None
    else:
        return None


def get_zipcode_helper(df, geolocator, lat_field, lon_field):
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    df['zipcode'] = location.raw['address']['postcode']
    return df


def clean_zipcode_helper(x):
    if len(x) > 5:
        return x[0:5]
    else:
        return x.zfill(5)


def clean_data_helper(df):
    df = df[df['state'] == 'NY']
    df['price'] = df['price'].apply(convert_currency_to_float)
    df['price'] = pd.to_numeric(df['price'])

    df['weekly_price'] = df['weekly_price'].apply(convert_currency_to_float)
    df['weekly_price'] = pd.to_numeric(df['weekly_price'])

    df['monthly_price'] = df['monthly_price'].apply(convert_currency_to_float)
    df['monthly_price'] = pd.to_numeric(df['monthly_price'])

    df['security_deposit'] = df['security_deposit'].apply(convert_currency_to_float)
    df['security_deposit'] = pd.to_numeric(df['security_deposit'])

    df['cleaning_fee'] = df['cleaning_fee'].apply(convert_currency_to_float)
    df['cleaning_fee'] = df['cleaning_fee'].fillna('0.00')
    df['cleaning_fee'] = pd.to_numeric(df['cleaning_fee'])

    df['extra_people'] = df['extra_people'].apply(convert_currency_to_float)
    df['extra_people'] = pd.to_numeric(df['extra_people'])

    df['guests_included'] = pd.to_numeric(df['guests_included'])

    df['zipcode'] = df['zipcode'].apply(clean_zipcode_helper)
    for date_col in date_column:
        df[date_col] = df[date_col].apply(convert_string_to_date)
    return df


def read_csv_main():
    df_list = []

    # read the first csv file
    first_csv_path = os.path.join(current_dir, 'DA-master', 'listings file 1 of 4.csv')
    conv_dir = {x: str for x in date_column + str_column}
    df = pd.read_csv(first_csv_path, converters=conv_dir)
    col_list = df.columns
    df_list.append(df)

    # read the remaining csv files
    remaining_file_list = ['listings file 2 of 4.csv', 'listings file 3 of 4.csv', 'listings file 4 of 4.csv']
    for file in remaining_file_list:
        file_path = os.path.join(current_dir, 'DA-master', file)
        df = pd.read_csv(file_path, header=None, converters={3: str, 22: str, 43: str, 75: str,
                                                             77: str, 78: str, 87: str, 88: str}, names=col_list)
        df_list.append(df)
    df_total = pd.concat((df_list), axis=0, ignore_index=True)

    # fill blank value in column zipcode
    df_missing_zip = df_total[(df_total['zipcode'] == '') & (df_total['latitude'].notnull()) &
                              (df_total['longitude'].notnull())]
    if not df_missing_zip.empty:
        print('Start filling zipcode through geopy')
        df_total = df_total[df_total['zipcode'] != '']
        df_missing_zip = df_missing_zip.apply(get_zipcode_helper, axis=1, geolocator=geolocator, lat_field='latitude',
                                              lon_field='longitude')
        df_total = pd.concat([df_total, df_missing_zip])
        print('Finish filling')

    input_list = np.array_split(df_total, 10)
    num_processes = cpu_count() - 1
    result = []
    with tqdm(total=len(input_list)) as pbar:
        with Pool(processes=num_processes) as pool:
            for chunk in pool.imap_unordered(clean_data_helper, input_list):
                pbar.update(n=1)
                result.append(chunk)
    df_clean = pd.concat(result)
    records = []

    # create directory to save pickles
    if not os.path.exists(os.path.join(current_dir, 'Cleaned_AirBnB')):
        os.mkdir(os.path.join(current_dir, 'Cleaned_AirBnB'))

    for zip_code, sub_df in df_clean.groupby('zipcode'):
        records.append([zip_code, len(sub_df)])
        if len(sub_df) >= 50:
            file_name = f'Zip_{zip_code}.pkl'
            sub_df.to_pickle(os.path.join(current_dir, 'Cleaned_AirBnB', file_name))
        else:
            print(f'{zip_code} only has {len(sub_df)} records')
    df_records = pd.DataFrame(records, columns=['zipcode', 'records_num'])
    df_records.to_excel(os.path.join(current_dir, 'Inventory.xlsx'), index=False)

    print('Done')


if __name__ == "__main__":
    warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)
    read_csv_main()
