import io
import time
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta

def codeListing() :

    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_params = {
        'locale': 'ko_KR',
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT04601'
    }
    headers = {'Referer' : 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}
    otp = requests.post(otp_url, params = otp_params, headers = headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_params = {'code' : otp}
    response = requests.post(down_url, params = down_params, headers = headers)

    data = pd.read_csv(io.BytesIO(response.content), encoding = 'euc-kr', dtype = {'단축코드': 'string'})

    return data

def PDFListing(isuCd, code, name, date) :

    headers = {'Referer' : 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader'}

    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_params = {
        'locale': 'ko_KR',
        'tboxisuCd_finder_secuprodisu1_0' : f'{code}/{name}',
        'isuCd': f'{isuCd}',
        'isuCd2': f'{isuCd}',
        'codeNmisuCd_finder_secuprodisu1_0': f'{name}',
        'param1isuCd_finder_secuprodisu1_0': "",
        'trdDd': f'{date}',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT05001'
        }

    otp = requests.post(otp_url, params = otp_params, headers = headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_params = {'code' : otp}
    response = requests.post(down_url, params = down_params, headers = headers)

    data = pd.read_csv(io.BytesIO(response.content),
                       encoding = 'euc-kr',
                       dtype = {'단축코드' : str})

    return data

def dataCrawlling(codeList, date):


    for i, (isuCd, code, name) in enumerate(zip(codeList['표준코드'], codeList['단축코드'], codeList['한글종목약명'])):

        if i == 0:
            data = PDFListing(isuCd, code, name, date)
            data.insert(0, 'ETF코드', code)
            data = data.drop('시가총액', axis=1)
            time.sleep(0.5)

        else :
            tmp = PDFListing(isuCd, code, name, date)
            tmp.insert(0, 'ETF코드', code)
            tmp = tmp.drop('시가총액', axis=1)

            data = pd.concat([data, tmp])
            time.sleep(0.5)
        


    data.columns = ['etf_code', 'stock_code', 'stock_nm', 'stock_amn', 'evl_amt', 'ratio']

    return data.reset_index(drop = True)


if __name__ == '__main__' :

    # 코드 불러오기
    codeList = codeListing()
    codeList = codeList[(codeList['기초시장분류'] == '국내') & (codeList['기초자산분류'] == '주식')]


    # 오늘 날짜 세팅
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    new_date = now.strftime('%Y%m%d')

    data = dataCrawlling(codeList, new_date)
    data.to_json('new_data.json')

    old_date = now - timedelta(days = 7)
    old_date = old_date.strftime('%Y%m%d')
    data = dataCrawlling(codeList, old_date)
    data.to_json('old_data.json')
