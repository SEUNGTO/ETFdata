import requests
import pandas as pd
import FinanceDataReader as fdr
from tqdm import tqdm


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


def calcurate_etf_target_ewm(code, ewm):

    # 개별 ETF들의 가중평균 목표가를 구하는 공간

    etf_data = load_etf_data('new', code)
    stock_list = etf_data['종목코드'].tolist()

    tmp = pd.DataFrame({})
    for j, stock in enumerate(stock_list):

        try:
            if stock in ewm.keys():
                aa = pd.DataFrame(pd.Series(ewm[stock]), columns=[stock])
                # 목표가가 주어지기 이전 시점에는 종가로 대체
                close = fdr.DataReader(stock, start=start, end=end)
                close = pd.DataFrame(close['Close'].values, columns=[stock], index=close.index.astype(str))
                aa = aa.fillna(close)

                # 해당 종목의 가중치(보유비중) 반영
                aa = aa * (etf_data.loc[etf_data['종목코드'] == stock, '비중'].values[0]) / 100
                tmp = pd.concat([tmp, aa], axis=1)

            else:
                # 목표가가 아예 없는 종목은 종가로 대체
                aa = fdr.DataReader(stock, start=start, end=end)
                aa = pd.DataFrame(aa['Close'].values, columns=[stock], index=aa.index.astype(str))

                # 해당 종목의 가중치(보유비중) 반영
                aa = aa * (etf_data.loc[etf_data['종목코드'] == stock, '비중'].values[0]) / 100
                tmp = pd.concat([tmp, aa], axis=1)

        except:
            # Finance Data Reader에도 없는 종목은 pass
            continue

    return pd.DataFrame(tmp.sum(axis=1), columns=[code])


if __name__ == "__main__" :
    codeList = load_codeList()
    codeList = codeList[codeList['Type'] == 'ETF']
    ewm = load_ewm_data()

    start = min(ewm['005930'].keys())
    end = max(ewm['005930'].keys())

    etf_target_price = pd.DataFrame([])

    for code in tqdm(codeList['Symbol']) :

        data = calcurate_etf_target_ewm(code, ewm)
        pd.concat([etf_target_price, data])

    etf_target_price.to_json('etf_target_price.json')
