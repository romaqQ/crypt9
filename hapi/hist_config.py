### Historical Data Config ###
import datetime as dt
from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError

SYMBOLS_PATH = '../data/symbols'

HIST_DICT = {'binance':{'base_url':'https://data.binance.vision/', 
                        'symbols_url':{'spot':"https://api.binance.com/api/v3/exchangeInfo", 
                                       'um':"https://fapi.binance.com/fapi/v1/exchangeInfo",
                                       'cm':"https://dapi.binance.com/dapi/v1/exchangeInfo"}}}
    

# Date Combinations
YEARS = ['2017', '2018', '2019', '2020', '2021']
INTERVALS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1mo"]
DAILY_INTERVALS = ["1m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d"]
TRADING_TYPE = ["spot", "um", "cm"]
MONTHS = list(range(1,13))
MAX_DAYS = 35
START_DATE = dt.date(int(YEARS[0]), MONTHS[0], 1)
END_DATE = dt.datetime.now().date()

FIAT_QUOTE = ['EUR', 'USD', 'GBP', 'YEN', 'RUB']
TOKEN_QUOTE = ['BTC', 'ETH', 'USDT', 'USDC']




"""
def get_parser(parser_type):
  parser = ArgumentParser(description=("This is a script to download historical {} data").format(parser_type), formatter_class=RawTextHelpFormatter)
  parser.add_argument(
      '-s', dest='symbols', nargs='+',
      help='Single symbol or multiple symbols separated by space')
  parser.add_argument(
      '-y', dest='years', default=YEARS, nargs='+', choices=YEARS,
      help='Single year or multiple years separated by space\n-y 2019 2021 means to download {} from 2019 and 2021'.format(parser_type))
  parser.add_argument(
      '-m', dest='months', default=MONTHS,  nargs='+', type=int, choices=MONTHS,
      help='Single month or multiple months separated by space\n-m 2 12 means to download {} from feb and dec'.format(parser_type))
  parser.add_argument(
      '-d', dest='dates', nargs='+', type=match_date_regex,
      help='Date to download in [YYYY-MM-DD] format\nsingle date or multiple dates separated by space\ndownload past 35 days if no argument is parsed')
  parser.add_argument(
      '-startDate', dest='startDate', type=match_date_regex,
      help='Starting date to download in [YYYY-MM-DD] format')
  parser.add_argument(
      '-endDate', dest='endDate', type=match_date_regex,
      help='Ending date to download in [YYYY-MM-DD] format')
  parser.add_argument(
      '-folder', dest='folder', type=check_directory,
      help='Directory to store the downloaded data')
  parser.add_argument(
      '-c', dest='checksum', default=0, type=int, choices=[0,1],
      help='1 to download checksum file, default 0')
  parser.add_argument(
      '-t', dest='type', default='spot', choices=TRADING_TYPE,
      help='Valid trading types: {}'.format(TRADING_TYPE))

  if parser_type == 'klines':
    parser.add_argument(
      '-i', dest='intervals', default=INTERVALS, nargs='+', choices=INTERVALS,
      help='single kline interval or multiple intervals separated by space\n-i 1m 1w means to download klines interval of 1minute and 1week')


  return parser

"""
