from itertools import chain
from collections import defaultdict
from functools import partial
import pandas as pd
import os



#================================================================================
#   UTILITIES FOR RENAMING COLUMNS
#================================================================================

column_names = {
    'Annual profit (last year)': 'annual_profit',
    'Annual revenue (last year)': 'annual_revenue',
    'Average volume (3 months)': 'average_volume',
    'Day’s range': 'days_range',
    'Dividend yield': 'dividend_yield',
    # 'EPS forecast (this quarter)': 'eps_forecast',
    'Earnings growth (last year)': 'earnings_growth_last_yr',
    'Earnings growth (next 5 years)': 'earnings_growth_next_5_yr',
    'Earnings growth (this year)': 'earnings_growth_this_yr',
    'Industry': 'industry',
    # 'Last reporting date': 'last_reporting_date',
    'Market cap': 'market_cap',
    'Net profit margin': 'net_profit_margin',
    # 'Next reporting date': 'next_reporting_date',
    'P/E ratio': 'pe_ratio',
    'Previous close': 'previous_close',
    'Price/Book': 'price_book',
    'Price/Sales': 'price_sales',
    'Revenue growth (last year)': 'revenue_growth',
    'Sector': 'sector',
    'Today’s open': 'todays_open',
    'Volume': 'volume'
}

def rename_columns(df):

    df = df.rename(columns=column_names)
    df = df.loc[:, list(column_names.values())]
    return df


#================================================================================
#   UTILITIES FOR CHANGING COLUMN TYPE CONVERSIONS
#================================================================================

def catch(func):
    def wrapper(s):
        if s == '--':
            return pd.np.nan
        try:
            return func(s)
        except Exception as e:
            print("[{}]   Function:{}    Args:{}".format(e, func.__name__, s))
            return pd.np.nan
    
    return wrapper

@catch
def annual_profit(string):
    if string == '--':
        return pd.np.nan
    string = string.replace('$', '')
    amt = string[:-1]
    multiplier = string[-1]
    multiplier = {'M': 1e6,
                  'B': 1e9,
                  'K': 1e3}.get(string[-1], 1)
    
    return float(amt) * multiplier

@catch
def percent(s):
    return float(s.replace('%', ''))

@catch
def plus_minus_percent(s):
    s = s.replace(',', '').replace('%', '')
    _sign = {'+':1, '-': -1}
    sign = _sign[s[0]]
    num = float(s[1:])
    return num * sign

@catch
def dollar_sign(sg):
    lambda s: float(s.replace('$', ''))


column_converters = {
    'annual_profit': annual_profit,
    'annual_revenue': annual_profit,
    'dividend_yield': percent,
    # 'eps_forecast': dollar_sign,
    'earnings_growth_this_yr': plus_minus_percent,
    'earnings_growth_last_yr': plus_minus_percent,
    'earnings_growth_next_5_yr': plus_minus_percent,
    'market_cap': annual_profit,
    'net_profit_margin': percent,
    'revenue_growth': plus_minus_percent,
    'pe_ratio': catch(float)
}


    
def format_columns(df):
    df = df.copy()
    
    for col_name, func in column_converters.items():
        df[col_name] = df[col_name].apply(func)    

    return df

#================================================================================
#   UTILITIES FOR READING / LOADING FILES
#================================================================================

csv_args = {
    'index_col': ['Symbol'],
#     'names': column_names
}


class DataLoader(pd.Series):
    '''
    Utility to allow slicing of the files in a directory

    Get a list of AAPL's trading details file

        data_files.loc[:'AAPL', 'todays_trading']


    Get all the profile files:

        data_files.loc[:, 'profile']

    Use `.dx` instead of `.loc` to return the results in a joined dataframe

    '''

          
    @property
    def df(self):
        return Reader(self)
    
    @classmethod
    def from_directory(cls, directory=None):
        df = pd.DataFrame(list(get_filename_data(directory)))
        df.set_index(['symbol', 'category'], inplace=True)
        series_data = df['filename'].to_dict()
        return cls(series_data) 


class Reader:
    '''
    Reads CSV files and returns dataframe
    '''
    def __init__(self, indexer):
        self.indexer = indexer

    def __getitem__(self, *item):
        filenames = self.indexer.loc[item]
        if hasattr(filenames, 'upper'):
            filenames = [filenames]
        return merge_dataframes(filenames)
        

    
# def df_to_series(df):
#     series = df.set_index('0')['1']
#     series = series[~series.index.duplicated()]
#     return series

    
# def fill_with_value(row):
#     r = [v for v in row if pd.notnull(v)]
#     return r[0] if r else pd.np.nan


# def flatten_col(df): 
#     return df.apply(func=fill_with_value, axis=0)

    
# def flatten(df):
#     _df = {}
#     for symbol in df.index.values:
#         _df[symbol] = flatten_col(df.loc[symbol])
#     return pd.DataFrame(_df).transpose()
    

def split_filename(filename):
    return tuple(filename.split('/')[-1].replace('.csv', '').split('_', 1))


def get_filename_data(directory=None):
    filenames = os.listdir(directory)
    filenames = [os.path.join(directory, fn) for fn in filenames]
    
    for fn in filenames:
        symbol, category = split_filename(fn)
        yield {
            'filename': fn,
            'symbol': symbol,
            'category': category,
        }


def merge_dataframes(filenames):
    ds = []
    for fn in filenames:
        d = pd.read_csv(fn, **csv_args)
        ds.append(d)   

    index = list(chain(*map(lambda x: set(x.index.values), ds)))
    columns = list(chain(*map(lambda x: set(x.columns.values), ds)))
    
    df_container = pd.DataFrame(index=set(index), columns=set(columns))

    for d in ds:
        df_container.loc[d.index, d.columns] = d
    return df_container

