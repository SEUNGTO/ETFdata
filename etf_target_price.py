import requests
import pandas as pd
import re
import FinanceDataReader as fdr

def calcurate_target_price(researchData) :

    researchData['목표가'] = [re.sub("\D", "", v) for v in researchData['목표가']]
    researchData = researchData[researchData['목표가'] != ""]
    researchData.loc[:, '게시일자'] = researchData['게시일자'].apply(lambda x : x.replace(".", ""))
    researchData.loc[:, '게시일자'] = pd.to_datetime(researchData.loc[:, '게시일자'])
    researchData.loc[:, '목표가'] = researchData.loc[:, '목표가'].astype(float)

    pivot = researchData.pivot_table(index = '게시일자', columns = '종목코드', values = '목표가', aggfunc= 'mean')
    pivot = pivot.astype(float)

    start = researchData.loc[:, '게시일자'].min()
    end = researchData.loc[:, '게시일자'].max()

    period = pd.date_range(start = start, end = end, freq = 'D')
    bs_data = pd.DataFrame([], index = period)
    bs_data = bs_data.merge(pivot, left_index = True, right_index = True)

    ewmdata = bs_data.ewm(span = 90, adjust = False).mean()
    ewmdata.index = ewmdata.index.astype(str)
    ewmdata.reset_index(inplace = True)
    ewmdata = ewmdata.rename(columns = {'index' : 'Date'})

    return ewmdata

def load_research() :
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/research.json'
    response = requests.get(url).json()

    return response

def calcurate_etf_target_price() :
    research = load_research()
    research = pd.DataFrame(research)
    ewmdata = calcurate_target_price(research)

    start = ewmdata[['Date']].min().values[0]
    end = ewmdata[['Date']].max().values[0]

    codeList = ewmdata.columns
    codeList = codeList.drop('Date')

    price_data = pd.DataFrame({})
    for code in codeList :
        tmp = fdr.DataReader(code, start = start, end = end)['Close']
        tmp.fillna(method = 'bfill', inplace = True)
        tmp.name = code
        price_data = pd.concat([price_data, tmp], axis = 1)

    # index 속성 맞추기
    ewmdata.index = ewmdata['Date']
    price_data.index = [str(idx)[:10] for idx in price_data.index]
    ewmdata.fillna(price_data, inplace = True)
    ewmdata.fillna(method = 'bfill', inplace = True)


    ### ETF 데이터 불러오기
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/new_data.json'
    response = requests.get(url)
    etf_data = pd.DataFrame(response.json())


    # 종가를 저장할 데이터 프레임 만들어두기  ## 로드시간 최소화
    stock_mkt_price = pd.DataFrame({})

    # ETF별로 구한 최종 목표가를 저장할 데이터 프레임 생성
    etf_target_price = pd.DataFrame({})

    for etf_code in etf_data['etf_code'].unique() :

        tmp = etf_data[etf_data['etf_code'] == etf_code]

        # 목표가가 있는 경우
        in_ewm = [code for code in tmp['stock_code'] if code in ewmdata.columns]
        ratio = [tmp.loc[tmp['stock_code'] == stock_code, 'ratio'].values[0] for stock_code in in_ewm]
        in_ewm_price = ewmdata[in_ewm] * ratio / 100
        in_ewm_price = in_ewm_price.sum(axis = 1)

        # 목표가가 없는 경우 -> 종가로 대체
        out_ewm = [code for code in tmp['stock_code'] if code not in ewmdata.columns]
        out_ewm_price = pd.DataFrame({})

        for stock_code in out_ewm :
            if len(re.sub("\d", "", stock_code)) == 0 :
                try :
                    if stock_code in stock_mkt_price.columns :
                        out_ewm_price = pd.concat([out_ewm_price, stock_mkt_price[stock_code]], axis = 1)

                    elif stock_code not in stock_mkt_price.columns :
                        tmp_stock_price = fdr.DataReader(stock_code, start = start, end = end)
                        tmp_stock_price.index = [str(idx)[:10] for idx in tmp_stock_price.index]
                        tmp_stock_price = tmp_stock_price['Close']
                        tmp_stock_price.fillna(method = 'bfill', inplace = True)
                        tmp_stock_price.name = stock_code

                        out_ewm_price = pd.concat([out_ewm_price, tmp_stock_price], axis = 1)

                        # stock_mkt_price에도 저장
                        stock_mkt_price = pd.concat([stock_mkt_price, tmp_stock_price], axis = 1)

                except :
                    continue
            else :
                continue

        # 검색되는 경우만 load
        ratio = [tmp.loc[tmp['stock_code'] == stock_code, 'ratio'].values[0] for stock_code in out_ewm_price.columns]

        out_ewm_price = out_ewm_price * ratio / 100
        out_ewm_price = out_ewm_price.sum(axis = 1)

        # 합치기
        final = out_ewm_price + in_ewm_price
        final.name = etf_code

        etf_target_price = pd.concat([etf_target_price, final], axis = 1)

    return etf_target_price




if __name__ == '__main__' :

    research = load_research()
    research = pd.DataFrame(research)
    ewmdata = calcurate_target_price(research)

    start = ewmdata[['Date']].min().values[0]
    end = ewmdata[['Date']].max().values[0]

    codeList = ewmdata.columns
    codeList = codeList.drop('Date')

    price_data = pd.DataFrame({})
    for code in codeList :
        tmp = fdr.DataReader(code, start = start, end = end)['Close']
        tmp.fillna(method = 'bfill', inplace = True)
        tmp.name = code
        price_data = pd.concat([price_data, tmp], axis = 1)

    # index 속성 맞추기
    ewmdata.index = ewmdata['Date']
    price_data.index = [str(idx)[:10] for idx in price_data.index]
    ewmdata.fillna(price_data, inplace = True)
    ewmdata.fillna(method = 'bfill', inplace = True)


    ### ETF 데이터 불러오기
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/new_data.json'
    response = requests.get(url)
    etf_data = pd.DataFrame(response.json())


    # 종가를 저장할 데이터 프레임 만들어두기  ## 로드시간 최소화
    stock_mkt_price = pd.DataFrame({})

    # ETF별로 구한 최종 목표가를 저장할 데이터 프레임 생성
    etf_target_price = pd.DataFrame({})


    for etf_code in etf_data['etf_code'].unique() :

        tmp = etf_data[etf_data['etf_code'] == etf_code]

        # 목표가가 있는 경우
        in_ewm = [code for code in tmp['stock_code'] if code in ewmdata.columns]
        ratio = [tmp.loc[tmp['stock_code'] == stock_code, 'ratio'].values[0] for stock_code in in_ewm]
        in_ewm_price = ewmdata[in_ewm] * ratio / 100
        in_ewm_price = in_ewm_price.sum(axis = 1)

        # 목표가가 없는 경우 -> 종가로 대체
        out_ewm = [code for code in tmp['stock_code'] if code not in ewmdata.columns]
        out_ewm = [code for code in out_ewm if len(re.sub("\d", "", code)) == 0]

        out_ewm_price = pd.DataFrame({})

        for stock_code in out_ewm :
            try :
                if stock_code in stock_mkt_price.columns :
                    out_ewm_price = pd.concat([out_ewm_price, stock_mkt_price[stock_code]], axis = 1)

                elif stock_code not in stock_mkt_price.columns :
                    tmp_stock_price = fdr.DataReader(stock_code, start = start, end = end)
                    tmp_stock_price.index = [str(idx)[:10] for idx in tmp_stock_price.index]
                    tmp_stock_price = tmp_stock_price['Close']
                    tmp_stock_price.fillna(method = 'bfill', inplace = True)
                    tmp_stock_price.name = stock_code

                    out_ewm_price = pd.concat([out_ewm_price, tmp_stock_price], axis = 1)

                    # stock_mkt_price에 업데이트
                    stock_mkt_price = pd.concat([stock_mkt_price, tmp_stock_price], axis = 1)

            except :
                continue

        # 검색되는 경우만 load
        ratio = [tmp.loc[tmp['stock_code'] == stock_code, 'ratio'].values[0] for stock_code in out_ewm_price.columns]

        out_ewm_price = out_ewm_price * ratio / 100
        out_ewm_price = out_ewm_price.sum(axis = 1)

        # 합치기
        final = sum(out_ewm_price, in_ewm_price)
        final.name = etf_code

        etf_target_price = pd.concat([etf_target_price, final], axis = 1)
