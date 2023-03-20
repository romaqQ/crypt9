###- Format configurations for exchange websocket streams -###
from typing import Union
from typing import List

EXCHANGES = ['binance', 'kraken', 'coinbase']

### Parsing Functions ###
# Format Parsings #
def binance_format_parsing(sequence_input: Union[list, dict]):
    if isinstance(sequence_input, dict):
        return list(sequence_input.values)
    elif isinstance(sequence_input, list):
        if isinstance(sequence_input[0], dict):
            return [list(x.values()) for x in sequence_input]
        else:
            raise TypeError('Other types than dict or list[dict] not supported')

def kraken_format_parsing(sequence_input: Union[list, List[list]]):
    if isinstance(sequence_input[0], list):
        return [x + [y[2]] + [y[3]] for y in sequence_input for x in y[1]]
    elif isinstance(sequence_input[0], int):
        return [x + [sequence_input[2]] + [sequence_input[3]] for x in sequence_input[1]]
    else:
        raise TypeError

def coinbase_format_parsing(sequence_input:Union[list, dict]):
    return binance_format_parsing(sequence_input)

general_parsing_functions = [eval(f'{exch}_format_parsing') for exch in EXCHANGES]

def kraken_bbo_format_parsing(sequence_input: Union[list, List[list]]):
    if isinstance(sequence_input[0], list):
        return [y[1] for y in sequence_input]
    elif isinstance(sequence_input[0], int):
        return [sequence_input[1]]
    else:
        raise TypeError

def binance_ob_format_parsing(sequence_input: Union[list, dict]):
    if isinstance(sequence_input, dict):
        return [str(x) for x in sequence_input.values]
    elif isinstance(sequence_input, list):
        if isinstance(sequence_input[0], dict):
            return [[str(x) for x in x.values()] for x in sequence_input]
        else:
            raise TypeError('Other types than dict or list[dict] not supported')

def kraken_ob_format_parsing(sequence_input: Union[list, List[list]]):
    if isinstance(sequence_input[0], list):
        msg_list = []
        for msg in sequence_input:
            side_tuple = (1,2) if len(msg) == 4 else (1,3)
            type_idx = side_tuple[1] 
            for i in range(*side_tuple):
                for side, vals in msg[i].items():
                    if side != 'c':
                        msg_list.extend([[msg[type_idx]] + [side] + [str(vals)]])
        return msg_list

def coinbase_ob_format_parsing(sequence_input:Union[list, dict]):
    return binance_ob_format_parsing(sequence_input)

ob_parsing_functions = [eval(f'{exch}_ob_format_parsing') for exch in EXCHANGES]

bbo_parsing_functions = [eval(f'{exch}_{"bbo" if exch == "kraken" else "ob"}_format_parsing') for exch in EXCHANGES]

### Trades ###
TRADES = {exch:{} for exch in EXCHANGES}
for exch, parse_f in zip((EXCHANGES),(general_parsing_functions)):
    TRADES[exch]['parsing_function'] = parse_f

TRADES['binance']['table'] =  """create table {} 
            ([e_eventType] text,
            [E_eventTime] integer,
            [s_symbol] text,
            [t_tradeId] integer,
            [p_price] text,
            [q_quantity] text,
            [b_buyerOrderId] integer,
            [a_sellerOrderId] integer,
            [T_tradeTime] integer,
            [m_buyerMaker] bool,
            [M_ignore] bool)"""

TRADES['kraken']['table'] =  """create table {}
            ([price] real,
            [volume] real,
            [time] real,
            [side] text,
            [orderType] text,
            [misc] text,
            [channelName] text,
            [pari] text)"""

TRADES['coinbase'] = """create table {}
            ([type] text,
            [sequence] integer,
            [product_id] text,
            [price] text,
            [open_24h] text,
            [volume_24h] text,
            [low_24h] text,
            [high_24h] text,
            [volume_30d] text,
            [best_bid] text,
            [best_ask] text,
            [side] text,
            [time] text,
            [trade_id] integer,
            [last_size] text)"""

### Klines ###
KLINES = {exch:{} for exch in EXCHANGES}
for exch, parse_f in zip((EXCHANGES),(general_parsing_functions)):
    KLINES[exch]['parsing_function'] = parse_f
KLINES['binance']['table'] =  """create table {} 
            ([E_eventTime] integer,
            [t_kStartTime] integer,
            [T_kEndTime] integer,
            [s_symbol] text,
            [i_interval] text,
            [f_firstTradeId] integer,
            [L_lastTradeId] integer,
            [o_open] text,
            [c_close] text,
            [h_high] text,
            [l_low] text,
            [n_numberTrades] integer,
            [x_isKlineClosed] bool, 
            [v_baseAssetVolume] text,
            [q_quoteAssetVolume] text,
            [V_takerBuyBaseAssetVolume] text,
            [Q_takerBuyQuoteAssetVolume] text,
            [B_ignore] text)"""

### Symbol Ticker ###
TICKER = {exch:{} for exch in EXCHANGES}
TICKER['binance']['table'] =  """create table {} 
            ([e_eventType] text,
            [E_eventTime] integer,
            [s_symbol] text,
            [p_priceChange] text,
            [P_priceChangePercent] text,
            [w_weightedAvgPrice] text,
            [x_firstTradePrior24h] text,
            [c_lastPrice] text,
            [Q_lastQuantity] text,
            [b_bestBidPrice] text,
            [B_bestBidQuantity] text,
            [a_bestAskPrice] text,
            [A_bestAskQuantity] text,
            [o_openPrice] text,
            [h_highPrice] text,
            [l_lowPrice] text,
            [v_totalTradedBaseAssetVolume] text,
            [q_totalTradedQuoteAssetVolume] text,
            [O_statisticsOpenTime] integer,
            [C_statisticsCloseTime] integer,
            [F_firstTradeId] integer,
            [L_lastTradeId] integer,
            [n_totalNumberTrades] integer)"""

### BestBidBestAsk ###
BBO = {exch:{} for exch in EXCHANGES}
for exch, parse_f in zip((EXCHANGES),(bbo_parsing_functions)):
    BBO[exch]['parsing_function'] = parse_f
BBO['binance']['table'] =  """create table {} 
            ([u_orderBookUpdateId] integer,
            [s_symbol] text,
            [b_bestBidPrice] text,
            [B_bestBitQuantity] text,
            [a_bestAskPrice] text,
            [A_bestAskQuantity] text)"""

BBO['coinbase']['table'] =  """create table {} 
            ([type] text,
            [product_id] text,
            [updates/bids_snap] text,
            [time/asks_snap] text)"""

BBO['kraken']['table'] =  """create table {} 
            ([best_bid] text,
            [best_ask] text,
            [time] text,
            [bid_volume] text,
            [ask_volume] text)"""

### OrderBook Partial Updates ###
OB_PARTIAL = {exch:{} for exch in EXCHANGES}
OB_PARTIAL['binance']['table'] =  """create table {} 
            ([lastUpdateId] text,
            [bid_priceLevel] text,
            [bid_quantityUpdate] text,
            [ask_priceLevel] text,
            [ask_quantityUpdate] text)"""

### OrderBook Depth Updates ###
OB = {exch:{} for exch in EXCHANGES}
for exch, parse_f in zip((EXCHANGES),(ob_parsing_functions)):
    OB[exch]['parsing_function'] = parse_f
OB['binance']['table'] =  """create table {} 
            ([e_eventType] text,
            [E_eventTime] text,
            [s_symbol] text,
            [U_firstUpdateIdEvent] text,
            [u_finalUpdateIdEvent] text,
            [bid_bidUpdates] text,
            [ask_askUpdates] text)"""

OB['coinbase']['table'] =  """create table {} 
            ([type] text,
            [product_id] text,
            [updates/bids_snap] text,
            [time/asks_snap] text)"""

OB['kraken']['table'] =  """create table {} 
            ([type] text,
            [side] text,
            [updates] text)"""
CHANNELS_DICT = {'trade':TRADES, 'bbo':BBO, 'all_bbo':BBO, 'klines':KLINES, 'ob':OB}
