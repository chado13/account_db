from datetime import datetime, timedelta
import pandas as pd
import sqlite3
from kiwooma.api import EasyAPI
import sys

kiwooma = EasyAPI()
today = datetime.strftime(datetime.today().date(), format = '%Y-%m-%d')
con = sqlite3.connect('E:/database/account_db/AH_LAB_Manager.db')
cursor = con.cursor()

accnos = kiwooma.get_account_no()
calendar = pd.read_pickle('E:/AH_Lab/05_AH_Application/anTrader/data/KRX_calendar.pkl')
if today in calendar:
    date = calendar.loc[today]
    today = datetime.strptime(today, '%Y-%m-%d').date()
else:
    today= df["Date"].iloc[-1]
    today = datetime.strptime(today, '%Y-%m-%d %H:%M:%S').date()
    date = calendar.loc[today].date()

yester_day=calendar.iloc[calendar.index.get_loc(today)-1]
yester_day= datetime.strftime(yester_day, format='%Y-%m-%d %H:%M:%S')
date = datetime.strftime(date, format='%Y-%m-%d %H:%M:%S') 

p_df = pd.read_sql_query("""
                SELECT Date, Accno, Algorithm, TotalAsset ,CumRealPNL, CumPNL, CumReturn FROM AlgorithmInfo WHERE Date >= '{}'
            """.format(yester_day).strip(), con)
p_df = p_df.groupby('Accno').last()

if isinstance(accnos, str):
    accnos = [accnos]

for accno in accnos:
    kiwooma.register_account_no(accno)
    deposit = kiwooma.get_deposit_detail()['d+2추정예수금'] #availableCash
    total_asset = kiwooma.get_account_balance()['추정예탁자산'] #TotalAsset
    holding_profit = kiwooma.get_account_balance()['총평가손익금액'] #실현되지 않은 손익
    today_real_pnl = kiwooma.get_today_realized_pnl() #오늘 실현손익
 
    try:   
        yest_asset = p_df.loc[accno, 'TotalAsset']
        yest_cumrealpnl = p_df.loc[accno, 'CumRealPNL'] #어제의 누적실현손익금액
        yest_cumpnl = p_df.loc[accno, 'CumPNL'] #어제의 누적수익금
        yest_cumreturn = p_df.loc[accno, 'CumReturn'] #어제의 누적수익률
    except KeyError:
        yest_asset = total_asset 
        yest_cumrealpnl = 0
        yest_cumpnl = 0
        yest_cumreturn = 0

    cum_real_pnl = yest_cumrealpnl + float(today_real_pnl) #누적실현손익 = 어제의 누적실현손익 + 오늘의 실현손익
    cum_pnl = cum_real_pnl + holding_profit # 누적수익금 - 누적실현손익 + 오늘의 실현되지 않은 총평가손익금
    today_pnl = cum_pnl - yest_cumpnl #당일수익금

    try:    
        today_return = round(today_pnl/(yest_asset-today_pnl),4) # 당일계좌수익률 = 당일수익금/어제의 추정예탁자산
    except ZeroDivisionError:
        today_return = 0

    cum_return = round((1+yest_cumreturn)*(1+today_return)-1, 4) #누적계좌수익률: (1+어제의 누적수익률) * (1+당일수익률) - 1
    datetime = datetime.strftime(datetime.today(), format='%Y-%m-%d %H:%M:%S')
    algorithm = '아이언맨'

    today_data = (datetime, accno, algorithm ,deposit, total_asset, today_real_pnl  ,cum_real_pnl, today_pnl, cum_pnl, today_return, cum_return)

    cursor.execute('''INSERT INTO AlgorithmInfo (Date, Accno, Algorithm, AvailableCash,TotalAsset, TodayRealPNL, 
                        CumRealPNL, TodayPNL, CumPNL,TodayReturn, CumReturn) VALUES(?,?,?,?,?,?,?,?,?,?,?)''', today_data)


    con.commit()