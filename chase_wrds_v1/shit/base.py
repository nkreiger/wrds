import accessor
import numpy as np
import pandas as pd
import wrds
import datetime
import matplotlib.pyplot as plt

Ticker = 'CAH'


# This function pulls data for each fiscal quarter defined in the "fpi_list....

def getData(Ticker, list):
    df = pd.DataFrame([])
    for x in list:
        df = df.append(accessor.getConnection().raw_sql("""
                                                SELECT TICKER, ACTDATS, FPEDATS, ANALYS, ESTIMATOR, FPI, value,
                                                ACTUAL FROM ibes.det_epsus WHERE usfirm=1 
                                                and oftic='""" + Ticker + """' 
                                                and FPI='""" + x + """'
                                                """), ignore_index=True)
    accessor.closeConnection()
    return df


fpi_list = ['6', '7', '8', '9', 'N']
df = getData(Ticker, fpi_list)


pd.to_datetime(df['actdats'], format='%Y/%m/%d')
pd.to_datetime(df['fpedats'], format='%Y/%m/%d')
pd.to_numeric(df['value'], downcast='float')

df = df.pivot_table(values=['value'],
                    index=['fpedats', 'actdats'],
                    columns=['estimator', 'analys'],
                    dropna=True)

df.replace('NaN', np.nan, inplace=True)
df = df.groupby(level='fpedats').fillna(method='ffill')
df['Mean'] = df.mean(axis=1)
df['Max'] = df.max(axis=1)
df['Min'] = df.min(axis=1)
df['Count'] = df.count(axis=1)
df['Stdev'] = df.std(axis=1)

df3 = df['Mean']
df3 = pd.DataFrame(df3)

df3 = pd.DataFrame(df3).reset_index()
df3 = pd.pivot_table(data=df3, values='Mean', index='actdats', columns='fpedats')

df3 = df3.apply(lambda series: series.loc[:series.last_valid_index()].ffill())

df3.index = pd.to_datetime(df3.index)
df3 = df3.resample('W').last()
print(df3)

df3 = df3.apply(lambda series: series.loc[:series.last_valid_index()].ffill())


def ROC(cp, tf):
    roc = []
    x = tf
    while x < len(cp):
        rocs = (cp[x] - cp[x - tf]) / cp[x - tf]

        roc.append(rocs)
        x += 1
    return roc



weight_1M = 0.04
weight_2M = 0.06
weight_3M = 0.08
weight_1Q = 0.12
weight_1H = 0.13
weight_9M = 0.15
weight_1Y = 0.32
weight_2Y = 0.10

RoC_1M = df3.apply(lambda series: series.loc[:series.last_valid_index()].pct_change(periods=4).multiply(weight_1M))
RoC_2M = df3.apply(lambda series: series.loc[:series.last_valid_index()].pct_change(periods=8).multiply(weight_2M))
RoC_3M = df3.apply(lambda series: series.loc[:series.last_valid_index()].pct_change(periods=12).multiply(weight_3M))
RoC_1Q = df3.apply(lambda series: series.loc[:series.last_valid_index()].pct_change(periods=16).multiply(weight_1Q))
RoC_1H = df3.apply(lambda series: series.loc[:series.last_valid_index()].pct_change(periods=31).multiply(weight_1H))
RoC_9M = df3.apply(lambda series: series.loc[:series.last_valid_index()].pct_change(periods=40).multiply(weight_9M))
RoC_1Y = df3.apply(lambda series: series.loc[:series.last_valid_index()].pct_change(periods=52).multiply(weight_1Y))
RoC_2Y = df3.apply(lambda series: series.loc[:series.last_valid_index()].pct_change(periods=104).multiply(weight_2Y))

Score = RoC_1M.add(RoC_2M).add(RoC_3M).add(RoC_1Q).add(RoC_1H).add(RoC_9M).add(RoC_1Y)
Score['Mean'] = Score.mean(axis=1)
Score.to_csv('est.csv')
Score.plot(legend=False)
plt.show()

# kleiwc15
# C1442759hase007!