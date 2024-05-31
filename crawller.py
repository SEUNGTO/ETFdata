import io
import time
import requests
import pandas as pd
import numpy as np
import pytz
from datetime import datetime, timedelta
import FinanceDataReader as fdr
import re
from bs4 import BeautifulSoup

################################### ETF 종목들의 코사인 유사도 계산 ###############################
def cosine_similarity_manual(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    return dot_product / (norm_vec1 * norm_vec2)

def compute_similarity() :
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/new_data.json'
    response = requests.get(url)
    data = pd.DataFrame(response.json())
    sim_data = data[['etf_code', 'stock_code', 'ratio']]
    pivot = sim_data.pivot_table(index = 'stock_code', columns = 'etf_code', values = 'ratio', aggfunc = 'sum')
    pivot = pivot.fillna(0)

    pivot_T = pivot.T

    cosine_sim_matrix = np.zeros((pivot_T.shape[0], pivot_T.shape[0]))

    # 코사인 유사도 계산
    for i in range(pivot_T.shape[0]):
        for j in range(pivot_T.shape[0]):
            cosine_sim_matrix[i, j] = cosine_similarity_manual(pivot_T.iloc[i].values, pivot_T.iloc[j].values)


    cosine_sim_df = pd.DataFrame(cosine_sim_matrix, index=pivot_T.index, columns=pivot_T.index)
    sim_dict = {}
    for code in pivot_T.index :
        sim_etfs_top5 = cosine_sim_df[code].sort_values(ascending=False).head(6)
        sim_etfs_top5 = sim_etfs_top5.index.tolist()
        sim_etfs_top5.remove(code)
        sim_dict[code] = sim_etfs_top5

    return pd.DataFrame(sim_dict)

########################################################################################



########################### 종목별 목표가 계산 #############################################
def calcurate_target_price(researchData) :

    researchData['목표가'] = [re.sub("\D", "", v) for v in researchData['목표가']]
    researchData = researchData[researchData['목표가'] != ""]
    researchData.loc[:, '게시일자'] = [v.replace(".", "") for v in researchData['게시일자']]
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

    return ewmdata
##########################################################################################

############### 리서치 데이터 업데이트 ################
def find_Recent_nid() :
    url = 'https://finance.naver.com/research/company_list.naver?&page=1'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    href = soup.find('div', class_ = 'box_type_m').find_all('a')[1].attrs['href']
    nid = re.sub(r"(.*)([0-9]{5,6})(.*)", "\g<2>", href)

    return nid

def researchCrawlling(nid) :
    result = {
        'stock_name': [],
        'code': [],
        'title': [],
        'nid': [],
        'target_price': [],
        'opinion': [],
        'date': [],
        'researcher': [],
        'link' : []
    }

    link = f'https://m.stock.naver.com/investment/research/company/{nid}'
    response = requests.get(link)
    soup = BeautifulSoup(response.content, 'html.parser')
    body = soup.find('div', class_ = 'ResearchContent_article__jjmeq')

    info = body.find('div', class_ = 'HeaderResearch_article__j3dPb')
    code = info.find('em', class_ = 'HeaderResearch_code__RmsRt').text
    stock_name = info.find('em', class_ = 'HeaderResearch_tag__7owlF').text
    stock_name = stock_name.replace(code, "")
    title = info.find('h3', class_ = 'HeaderResearch_title__cnBST').text
    researcher = info.find('cite', class_ = 'HeaderResearch_description__qH6Bs').text
    date = info.find('time', class_ = 'HeaderResearch_description__qH6Bs').text


    consensus = body.find('div', class_ = 'ResearchConsensus_article__YZ7oY')
    consensus = consensus.find_all('span', class_ = 'ResearchConsensus_text__XNJAT')
    opinion = consensus[0].text
    target_price = re.sub("\D", "", consensus[1].text)

    result['stock_name'].append(stock_name)
    result['code'].append(code)
    result['title'].append(title)
    result['nid'].append(nid)
    result['researcher'].append(researcher)
    result['date'].append(date)
    result['target_price'].append(target_price)
    result['opinion'].append(opinion)
    result['link'].append(link)
     
    return result

def load_research() :
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFdata/main/research.json'
    response = requests.get(url).json()
    research = pd.DataFrame(response)
    return research


def clear_old_research(research, period) :

    testee = research[['게시일자', 'nid']]
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    testee.loc[:, '게시일자'] = pd.to_datetime(testee['게시일자'], format='mixed').dt.tz_localize(tz)

    tt = now - timedelta(days = period)

    nid_list = testee[testee['게시일자'] >= tt]['nid']

    return research.loc[research['nid'].isin(nid_list), :]

###################################################################


################ 종목, ETF코드 업데이트 #################
def code_update() :

    stocks = fdr.StockListing('KRX')
    stocks = stocks.loc[:, ['Name', 'Code']]
    stocks.columns = ['Name', 'Symbol']
    stocks.loc[:, 'Type'] = 'Stock'
    
    etfs = fdr.StockListing('ETF/KR')
    etfs = etfs.loc[:, ['Name', 'Symbol']]
    etfs.loc[:, 'Type'] = 'ETF'
    
    code_list = pd.concat([stocks, etfs])
    
    return code_list.reset_index(drop = True)

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
############################################################################################################


############################### ETF PDF 크롤링################################
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
            data = data.drop(['시가총액', '시가총액 구성비중'], axis = 1)
            data.loc[:, '비중'] = data['평가금액']/data['평가금액'].sum() * 100
            time.sleep(0.5)

        else :
            tmp = PDFListing(isuCd, code, name, date)
            tmp.insert(0, 'ETF코드', code)
            tmp = tmp.drop(['시가총액', '시가총액 구성비중'], axis = 1)
            tmp.loc[:, '비중'] = tmp['평가금액']/tmp['평가금액'].sum() * 100
            data = pd.concat([data, tmp])
            time.sleep(0.5)


    data.columns = ['etf_code', 'stock_code', 'stock_nm', 'stock_amn', 'evl_amt', 'ratio']

    return data.reset_index(drop = True)

##############################################################################

if __name__ == '__main__' :

    # 코드 불러오기
    codeList = codeListing()
    codeList = codeList[(codeList['기초시장분류'] == '국내') & (codeList['기초자산분류'] == '주식')]


    # 오늘 날짜 세팅
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)

    # 오늘 날짜 데이터 수집
    new_date = now.strftime('%Y%m%d')
    data = dataCrawlling(codeList, new_date)
    data.to_json('new_data.json')

    # 7일 전 데이터 수집
    old_date = now - timedelta(days = 7)
    old_date = old_date.strftime('%Y%m%d')
    data = dataCrawlling(codeList, old_date)
    data.to_json('old_data.json')

    # 코드 업데이트
    code_list = code_update()
    code_list.to_json('code_list.json')


    # 리서치 데이터 업데이트
    research = load_research()
    research = clear_old_research(research, 180)

    nid_list = research['nid'].tolist()

    _last_nid = max(nid_list)
    _start_nid = str(int(_last_nid) + 1)
    _recent_nid = find_Recent_nid()

    for nid in range(int(_start_nid), int(_recent_nid)+1) :
        time.sleep(0.5)
        nid = str(nid)
        try :
            if nid == _start_nid :
                new_research = pd.DataFrame(researchCrawlling(nid))
            else  :
                tmp = pd.DataFrame(researchCrawlling(nid))
                new_research = pd.concat([new_research, tmp])
        except : continue
    new_research.columns = ['종목명', '종목코드', '리포트 제목', 'nid', '목표가', '의견', '게시일자', '증권사', '링크']
    research = pd.concat([research, new_research])
    research = research.reset_index(drop = True)
    research.to_json('research.json')

    # Exponential Weighted Moving Average 계산
   # ewmdata = calcurate_target_price(research)
  #  ewmdata.to_json('ewm_data.json')

