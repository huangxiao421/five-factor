#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @author: Xiao Huang

# importing #
import numpy as np
import pandas as pd
import wrds
from pandas.tseries.offsets import *

# 链接沃顿数据库 #
conn = wrds.Connection()

# 从Compustat数据库下载数据 #
comp = conn.raw_sql("""
                    select gvkey, datadate, at, pstkl, txditc, revt,cogs,tie,xsga,
                    pstkrv, seq, pstk
                    from comp.funda
                    where indfmt='INDL' 
                    and datafmt='STD'
                    and popsrc='D'
                    and consol='C' 
                    and datadate >= '01/01/1957'
                    """)

comp['datadate'] = pd.to_datetime(comp['datadate'])  # convert datadate to date fmt
comp['year'] = comp['datadate'].dt.year

# create preferrerd stock
comp['ps'] = np.where(comp['pstkrv'].isnull(), comp['pstkl'], comp['pstkrv'])
comp['ps'] = np.where(comp['ps'].isnull(), comp['pstk'], comp['ps'])
comp['ps'] = np.where(comp['ps'].isnull(), 0, comp['ps'])
comp['txditc'] = comp['txditc'].fillna(0)

# 生成be指标
comp['be'] = comp['seq'] + comp['txditc'] - comp['ps']
comp['be'] = np.where(comp['be'] > 0, comp['be'], np.nan)

# 生成op指标
comp['cogs'] = comp['cogs'].fillna(0)
comp['tie'] = comp['tie'].fillna(0)
comp['xsga'] = comp['xsga'].fillna(0)
comp['revt'] = comp['revt'].fillna(0)
comp['op'] = (comp['revt'] - comp['cogs'] - comp['tie'] - comp['xsga']) / comp['be']

# 生成at指标
comp['at'] = np.where(comp['at'] > 0, comp['at'], np.nan)

# 生成存在的累积年份
comp = comp.sort_values(by=['gvkey', 'datadate'])
comp['count'] = comp.groupby(['gvkey']).cumcount()
comp = comp[['gvkey', 'datadate', 'year', 'be', 'op', 'at', 'count']]

# 输出为csv文件
comp.to_csv('D:\\comp.csv', sep=',', header=True, index=True)

# 从CRSP数据库下载数据 #
crsp_m = conn.raw_sql("""
                      select a.permno, a.permco, a.date, b.shrcd, b.exchcd,
                      a.ret, a.retx, a.shrout, a.prc
                      from crsp.msf as a
                      left join crsp.msenames as b
                      on a.permno=b.permno
                      and b.namedt<=a.date
                      and a.date<=b.nameendt
                      where a.date between '07/01/1957' and '12/31/2013'
                      and b.exchcd between 1 and 3
                      """)

# 改变数值形式
crsp_m[['permco', 'permno', 'shrcd', 'exchcd']] = crsp_m[['permco', 'permno', 'shrcd', 'exchcd']].astype(int)

# 将日期改成当月的最后一天
crsp_m['date'] = pd.to_datetime(crsp_m['date'])
crsp_m['jdate'] = crsp_m['date'] + MonthEnd(0)

# 加入退市的收益
dlret = conn.raw_sql("""

                     select permno, dlret, dlstdt 
                     from crsp.msedelist
                     """)
dlret.permno = dlret.permno.astype(int)
dlret['dlstdt'] = pd.to_datetime(dlret['dlstdt'])
dlret['jdate'] = dlret['dlstdt'] + MonthEnd(0)
crsp = pd.merge(crsp_m, dlret, how='left', on=['permno', 'jdate'])
crsp['dlret'] = crsp['dlret'].fillna(0)
crsp['ret'] = crsp['ret'].fillna(0)
crsp['retadj'] = (1 + crsp['ret']) * (1 + crsp['dlret']) - 1
crsp['me'] = crsp['prc'].abs() * crsp['shrout']  # calculate market equity
crsp = crsp.drop(['dlret', 'dlstdt', 'prc', 'shrout'], axis=1)
crsp = crsp.sort_values(by=['jdate', 'permco', 'me'])

# 将同一个公司对应的多个股票代码的市场价值进行加总 #
crsp_summe = crsp.groupby(['jdate', 'permco'])['me'].sum().reset_index()
crsp_maxme = crsp.groupby(['jdate', 'permco'])['me'].max().reset_index()
crsp1 = pd.merge(crsp, crsp_maxme, how='inner', on=['jdate', 'permco', 'me'])
crsp1 = crsp1.drop(['me'], axis=1)
crsp2 = pd.merge(crsp1, crsp_summe, how='inner', on=['jdate', 'permco'])
crsp2 = crsp2.sort_values(by=['permno', 'jdate']).drop_duplicates()

# 将12月的市值调出
crsp2['year'] = crsp2['jdate'].dt.year
crsp2['month'] = crsp2['jdate'].dt.month
decme = crsp2[crsp2['month'] == 12]
decme = decme[['permno', 'date', 'jdate', 'me', 'year']].rename(columns={'me': 'dec_me'})

#
crsp2['ffdate'] = crsp2['jdate'] + MonthEnd(-6)
crsp2['ffyear'] = crsp2['ffdate'].dt.year
crsp2['ffmonth'] = crsp2['ffdate'].dt.month
crsp2['1+retx'] = 1 + crsp2['retx']
crsp2 = crsp2.sort_values(by=['permno', 'date'])

# 计算股票的累计收益率
crsp2['cumretx'] = crsp2.groupby(['permno', 'ffyear'])['1+retx'].cumprod()
# 滞后一项的累计收益率
crsp2['lcumretx'] = crsp2.groupby(['permno'])['cumretx'].shift(1)

# 滞后一项的市值
crsp2['lme'] = crsp2.groupby(['permno'])['me'].shift(1)

# 滞后一项的市值带来的缺失值用me/(1+retx)代替
crsp2['count'] = crsp2.groupby(['permno']).cumcount()
crsp2['lme'] = np.where(crsp2['count'] == 0, crsp2['me'] / crsp2['1+retx'], crsp2['lme'])

# 计算me的基础数据
mebase = crsp2[crsp2['ffmonth'] == 1][['permno', 'ffyear', 'lme']].rename(columns={'lme': 'mebase'})

# 输出mebase的数据
mebase.to_csv('D:\\mebase.csv', sep=',', header=True, index=True)

# 合并之前的数据，得到用于加权的市值wt
crsp3 = pd.merge(crsp2, mebase, how='left', on=['permno', 'ffyear'])
crsp3['wt'] = np.where(crsp3['ffmonth'] == 1, crsp3['lme'], crsp3['mebase'] * crsp3['lcumretx'])

# 输出crsp3的数据
crsp3.to_csv('D:\\crsp3.csv', sep=',', header=True, index=True)

decme['year'] = decme['year'] + 1
decme = decme[['permno', 'year', 'dec_me']]

# 每年6月的数据信息
crsp3_jun = crsp3[crsp3['month'] == 6]

crsp_jun = pd.merge(crsp3_jun, decme, how='inner', on=['permno', 'year'])
crsp_jun = crsp_jun[
    ['permno', 'date', 'jdate', 'shrcd', 'exchcd', 'retadj', 'me', 'wt', 'cumretx', 'mebase', 'lme', 'dec_me']]
crsp_jun = crsp_jun.sort_values(by=['permno', 'jdate']).drop_duplicates()

## 输出数据
crsp1.to_csv('D:\\crsp1.csv', sep=',', header=True, index=True)
crsp2.to_csv('D:\\crsp2.csv', sep=',', header=True, index=True)
crsp3_jun.to_csv('D:\\crsp3_jun.csv', sep=',', header=True, index=True)
crsp_jun.to_csv('D:\\crsp_jun.csv', sep=',', header=True, index=True)

#######################
# CCM Block           #
#######################

# 连接ccm数据库
ccm = conn.raw_sql("""
                  select gvkey, lpermno as permno, linktype, linkprim, 
                  linkdt, linkenddt
                  from crsp.ccmxpf_linktable
                  where substr(linktype,1,1)='L'
                  and (linkprim ='C' or linkprim='P')
                  """)

ccm['linkdt'] = pd.to_datetime(ccm['linkdt'])
ccm['linkenddt'] = pd.to_datetime(ccm['linkenddt'])
ccm['linkenddt'] = ccm['linkenddt'].fillna(pd.to_datetime('today'))

# 输出ccm原始数据
ccm.to_csv('D:\\ccm.csv', sep=',', header=True, index=True)

# 合并ｃｏｍｐ和ｃｃｍ数据
ccm1 = pd.merge(comp[['gvkey', 'datadate', 'be', 'count']], ccm, how='left', on=['gvkey'])
ccm1['yearend'] = ccm1['datadate'] + YearEnd(0)
ccm1['jdate'] = ccm1['yearend'] + MonthEnd(6)

# 选择连接合并的时间，得到有效的数据
ccm2 = ccm1[(ccm1['jdate'] >= ccm1['linkdt']) & (ccm1['jdate'] <= ccm1['linkenddt'])]
ccm2 = ccm2[['gvkey', 'permno', 'datadate', 'yearend', 'jdate', 'be', 'count']]

# 合并 comp和crsp数据库
ccm_jun = pd.merge(crsp_jun, ccm2, how='inner', on=['permno', 'jdate'])
ccm_jun['beme'] = ccm_jun['be'] * 1000 / ccm_jun['dec_me']

## 输出数据
ccm.to_csv('D:\\ccm.csv', sep=',', header=True, index=True)
ccm1.to_csv('D:\\ccm1.csv', sep=',', header=True, index=True)
ccm2.to_csv('D:\\ccm2.csv', sep=',', header=True, index=True)
ccm_jun.to_csv('D:\\ccm_jun.csv', sep=',', header=True, index=True)
