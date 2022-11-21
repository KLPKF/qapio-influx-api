

from qapio_influx_api.QapioInflux import InfluxSource
import pandas as pd, datetime as dt
from datetime import timedelta

data = InfluxSource("http://172.25.52.183", 8086,
                    'WCH5FSD4Qgo6oPBuvlvaLPhZ_cnLMQvQgMOBwUtDk_QjtrAIVbsN1UnbN1jTxdmAUo8M5wtyzXNYAkqHqZOh6g==', "KLPAM")

fromday = (pd.Timestamp.today() - timedelta(days=4))
today = pd.Timestamp.today()


def Vol90(fsym_id,Universet):

    testdata=data.dataset('SCREENINGDB',['Complete'],['VOLATILITY_90D'],fromday,today,{'FSYM_ID':fsym_id})
    print('Morten')
    return(testdata)


def Volatility(fsym_id):

    testdata=data.dataset('SCREENINGDB',['Complete'],['VOLATILITY'],fromday,today,{'FSYM_ID':fsym_id})

    return(testdata)


def msciuniv(universet):
    fromday = (pd.Timestamp.today() - timedelta(days=365))
    testdata=data.dataset('MSCIDB',[universet],['MSCI_SECURITY_CODE'],fromday,today)

    return(testdata)


def Beta(fsym_id, Universet):
    faktor='Beta_M_60M_USD_' + Universet
    testdata=data.dataset('SCREENINGDB',[Universet],[faktor],fromday,today,{'FSYM_ID':fsym_id})
    return(testdata)


def totret1dusd(fsym_id):

    testdata=data.dataset('FACTSETDB',fsym_id,['TOTAL_RETURN_1D_USD'],fromday,today)

    return(testdata)


def currencycrosses():

    testdata=data.dataset('FACTSETDB',['ref_fx'],['EXCH_RATE_PER_USD'],fromday,today)

    return(testdata)


def fromFACTSETDB(fsym_id,factor):


    testdata=data.dataset('SCREENINGDB',['Complete'],[factor],fromday,today,{'FSYM_ID':fsym_id})

    return(testdata)

def PRICE(fsym_id):

    testdata=data.dataset('FACTSETDB',fsym_id,['P_PRICE'],fromday,today)

    return(testdata)


def getIndex(index):
    conn = InfluxDb("172.25.52.182", 9999, "", "", "MSCIDB")
    today = pd.datetime.today()
    todate = (today - BDay(1))
    date_format = "%Y-%m-%d"
    from_value = dt.datetime.strftime(pd.to_datetime(todate), date_format)
    results = conn.query(['/./'], ["IDX_WEIGHT","MSCI_SECURITY_CODE","SEDOL"], from_value, from_value, [index])
    return results

def get_from_influx(fsym_id, field):
    data=InfluxSource("http://172.25.52.182", 9999,
                           'ccaZFzDwzQ_-BjCVl2B7E5x0J1Kqdmfonp4H7iChsKOMmm0QLjHtfn7k9acVvWwWWdxnqrekpSD-mQzrC_bYAg==', "KLPAM")

    fromday = (pd.Timestamp.today()-timedelta(days=5))
    today = (pd.Timestamp.today()-timedelta(days=1))

    print(field)

    testdata=data.dataset('FACTSETDB',fsym_id,[field],fromday,today)

    return(testdata)