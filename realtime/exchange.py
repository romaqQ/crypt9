### Formats Config ###
from typing import Union
from typing import List

### Exchange Connectivity Specifics ###
special_symbols = {'kraken':{'BTC':'XBT'}}
def binance_symbol(symbol:str, endpoint:str='wss'):
    if endpoint.lower() == 'wss':
        return f'{symbol.lower()}'
    return f'{symbol.lower()}'

def kraken_symbol(symbol:str, endpoint:str='wss'):
    symbol = symbol.upper()
    if endpoint.lower() == 'wss':
        return f'{symbol[:3]}/{symbol[3:]}' if '/' not in symbol else symbol
    return symbol

def coinbase_symbol(symbol:str, endpoint:str='wss'):
    symbol = symbol.upper()
    if endpoint.lower() == 'wss':
        return f'{symbol[:3]}-{symbol[3:]}'
    return f'{symbol[:3]}-{symbol[3:]}'

symbol_semantics = {'binance':binance_symbol,'coinbase':coinbase_symbol,'kraken':kraken_symbol}

def parse_symbols(symbol:str, exchange:str, endpoint:str='wss'):
    symbol = symbol.upper()
    if exchange in special_symbols.keys():
        for key, value in special_symbols[exchange].items():
            if key in symbol:
                symbol = symbol.replace(key, value)
    return symbol_semantics[exchange](symbol=symbol, endpoint=endpoint)

TRADE = {'coinbase':'ticker', 'binance':'trade', 'kraken':'trade'}
BBO = {'binance':'bookTicker', 'coinbase':'level2', 'kraken':'spread'} 
ALL_BBO = {'binance':'!bookTicker'}
OB = {'binance':'depth@100ms', 'coinbase':'level2', 'kraken':'book'}
USER = {'binance':'{}'}


CHANNEL_MAPPING = {
        'trade':TRADE,
        'bbo':BBO,
        'all_bbo':ALL_BBO,
        'ob':OB,
        'user':USER
        }

WSS_DICT = {
        'binance':{
            'api':'https://api.binance.com',
            'url':'wss://stream.binance.com:9443/ws',
            'sub':{'method':'SUBSCRIBE', 'params':['symbol@CHANNEL'], 'id':2},
            'succ_sub':'result',
            'succ_unsub':'null',
            'unsub':{'method':'UNSUBSCRIBE', 'params':['symbol@CHANNEL'], 'id':2},
            'channel':'CHANNEL'},
        'kraken':{
            'url':'wss://ws.kraken.com',
            'sub':{'event':'subscribe', 'pair':['symbol'], 'subscription':{'name':'CHANNEL'}},
            'succ_sub':'subscribe',
            'succ_unsub':'unsubscribe',
            'unsub':{'event':'unsubscribe', 'pair':['symbol'], 'subscription':{'name':'CHANNEL'}},
            'channel':'CHANNEL'},
        'coinbase':{
            'url':'wss://ws-feed.pro.coinbase.com',
            'sub':{'type':'subscribe', 'product_ids':['symbol'], 'channels':['CHANNEL']},
            'succ_sub':'subscriptions',
            'succ_unsub':'[]',
            'unsub':{'type':'unsubscribe', 'product_ids':['symbol'], 'channels':['CHANNEL']},
            'channel':'CHANNEL'}}

WSS_DICT_MULTIPLE = {
        'binance':{
            'url':'wss://stream.binance.com:9443/stream',
            'sub':{'method':'SUBSCRIBE', 'params':['symbol@CHANNEL'], 'id':2},
            'succ_sub':'result',
            'succ_unsub':'null',
            'unsub':{'method':'UNSUBSCRIBE', 'params':['symbol@CHANNEL'], 'id':2},
            'channel':'CHANNEL'},
        'kraken':{
            'url':'wss://ws.kraken.com',
            'sub':{'event':'subscribe', 'pair':['symbol'], 'subscription':{'name':'CHANNEL'}},
            'succ_sub':'subscribe',
            'succ_unsub':'unsubscribe',
            'unsub':{'event':'unsubscribe', 'pair':['symbol'], 'subscription':{'name':'CHANNEL'}},
            'channel':'CHANNEL'},
        'coinbase':{
            'url':'wss://ws-feed.pro.coinbase.com',
            'sub':{'type':'subscribe', 'product_ids':['symbol'], 'channels':['CHANNEL']},
            'succ_sub':'subscriptions',
            'succ_unsub':'[]',
            'unsub':{'type':'unsubscribe', 'product_ids':['symbol'], 'channels':['CHANNEL']},
            'channel':'CHANNEL'}}

def retrieve_singlestream_exchange_config(symbol:str, exchange:str, channel:str):
    symbol = parse_symbols(symbol=symbol, exchange=exchange)
    exch_instr_str = str(WSS_DICT[exchange])
    channel_parsed = CHANNEL_MAPPING[channel][exchange]
    for to_replace, replace in list(zip(('symbol', 'CHANNEL'), (symbol, channel_parsed))):
        exch_instr_str = exch_instr_str.replace(to_replace, replace)
    return eval(exch_instr_str)

EXCHANGE_DICT = {
        'binance':{'name': 'Binance'},
        'kraken':{'name': 'Kraken'},
        'coinbase':{'name':'Coinbase'}}

