import requests
import pandas as pd

def load_codeList() :
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/code_list.json'

    return pd.DataFrame(requests.get(url).json())

def load_etf_data(type, code) :

    if type == 'old' :
        url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/old_data.json'

    elif type == 'new' :
        url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/new_data.json'

    tmp = requests.get(url)
    tmp = pd.DataFrame(tmp.json(), dtype = str)

    tmp = tmp.loc[tmp['etf_code'] == code, :]
    tmp = tmp.drop('etf_code', axis = 1)
    tmp.columns = ['종목코드', '종목명', '보유량', '평가금액', '비중']
    tmp['보유량'] = tmp['보유량'].astype(float)
    tmp['평가금액'] = tmp['평가금액'].astype(float)
    tmp['비중'] = tmp['비중'].astype(float)

    return tmp

def load_ewm_data() :
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/ewm_data.json'
    ewm = requests.get(url).json()
    return ewm

def target_price_by_etf(stock_list, ewm):
    for j, stock in enumerate(stock_list):
        try:
            if j == 0:
                tmp = pd.DataFrame(pd.Series(ewm[stock]), columns=[stock])
                tmp = tmp * (etf_data.loc[etf_data['종목코드'] == stock, '비중'].values[0] / 100)
            else:
                aa = pd.DataFrame(pd.Series(ewm[stock]), columns=[stock])
                aa = aa * (etf_data.loc[etf_data['종목코드'] == stock, '비중'].values[0] / 100)
                tmp = pd.concat([tmp, aa], axis=1)
        except:
            continue

    return tmp.sum(axis=1)


if __name__ == "__main__" :
    codeList = load_codeList()
    codeList = codeList[codeList['Type'] == 'ETF']
    ewm = load_ewm_data()


    for i, code in enumerate(codeList['Symbol']) :
        try :
            if i == 0 :
                etf_data = load_etf_data('new', code)
                stock_list = etf_data['종목코드'].tolist()
                etf_target_price = target_price_by_etf(stock_list, ewm)
            else :
                etf_data = load_etf_data('new', code)
                stock_list = etf_data['종목코드'].tolis
                tmp = target_price_by_etf(stock_list, ewm)
                etf_target_price = pd.concat([etf_target_price, tmp])
        except :
            continue

    etf_target_price.to_json('etf_target_price.json')
