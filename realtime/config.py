### Base Config ###
import os

config = {
        'proxy':'',
        'http_proxy_host':'',
        'http_proxy_port':'',
        'dir_data':'../data/'}

ASYNC_SLEEP = 10e-3
CHANNEL_BUFFER_LIMITS = {'trade':[[1e3, 25], [1e4, 250]], 
                         'bbo':[[1e4, 50], [1e5, 1000]],
                         'all_bbo':[[1e6, 1000], [5e6, 50000]],
                         'ob':[[1e4, 50], [1e5, 1000]]}
DIR_STRUCTURE = {'root':os.path.join(os.sep,*os.getcwd().split(os.sep)[:-1])}
