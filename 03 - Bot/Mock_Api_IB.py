# -*- coding: utf-8 -*-

Mock_TS_Cour = "2022-03-10 16:00:00"

repHisto_1D = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\02 - Historiques quotidiens réajustés'
repHisto_15m_5m_1m = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\02 - Histo Bars Minutes réajustés'

NbSecWaitPour1minReelle = 20

import pandas as pd
import numpy as np
import time
import datetime
import argparse
import gc 
import collections
import inspect

import logging
import time as tm
import os.path

import threading

from scipy.stats.mstats import gmean
import talib as tb

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 300)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:0.1f}'.format

#import Bot

class Mock_IBApi:
    
    def __init__(self, bot):
        
        self.reqID  = 0
        self.listeReq = pd.DataFrame()
        #self.listeReq = None
        self.stop = False
        self.bot=bot
        


    def create_contract(self, NomContrat, EcheanceContrat):
        """ Creates an IB contract."""

        contract = Contract()

        #referentiel des contrats : https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php        
        
        if NomContrat=="DXM":
            contract.symbol = "DAX"
            contract.secType = "FUT"
            contract.exchange = "DTB"
            contract.currency = "EUR"
            contract.lastTradeDateOrContractMonth = EcheanceContrat  #202106
            contract.multiplier = "5"

        elif NomContrat=="YM":
            contract.symbol = "YM"
            contract.secType = "FUT"
            contract.exchange = "ECBOT"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = EcheanceContrat
            contract.multiplier = "5"

        elif NomContrat=="NQ":
            contract.symbol = "NQ"
            contract.secType = "FUT"
            contract.exchange = "GLOBEX"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = EcheanceContrat
            contract.multiplier = "20"
        else:
            print("Contrat non implémenté:", NomContrat)

        contract.nomContrat = NomContrat

        return contract


    def increment_id(self):
        """ Increments the request id"""

        self.reqID = self.reqID + 1
        

    def reqHistoricalData(self, reqID, contract, DateFinStrQuery, ProfondeurHistorique, TailleBougies, TypeInfos, Arg1, arg2, keepUpToDate:bool, arg3)  :
        ts = datetime.datetime.today()
        print(ts, threading.currentThread().getName(), "mock reqHistoricalData :")
       
        if DateFinStrQuery== '':
            DateFinStrQuery= datetime.datetime.strptime(Mock_TS_Cour, '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d %H:%M:%S')  
            
        self.listeReq = self.listeReq.append({'reqID':reqID, 'Contrat': contract.nomContrat , 'DateFin':DateFinStrQuery, 
                              'ProfHisto':ProfondeurHistorique, 'TailleBougies':TailleBougies, 'keepUpToDate':keepUpToDate}, ignore_index=True)
        print(self.listeReq)


    def historicalData(self, reqId:int, bar):
        ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
        #print(threading.currentThread().getName() , ts, "Mock historicalData - ReqId:", reqId, bar.date)

        self.bot.on_bar_update_histo(reqId, bar)
        
        
    def historicalDataUpdate(self, reqId: int, bar, TsReqDt):
        ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
        # print(threading.currentThread().getName() , ts, "Mock historicalDataUpdate - ReqId:", reqId, bar.date)
        
        #bot = gc.get_referrers(self) 
        F=(self.bot.cpt['reqId'] == reqId)
        # Contrat = self.bot.cpt.loc[F,'Contrat'].values[0]
        Periode = self.bot.cpt.loc[F,'Periode'].values[0]
        intPeriode = 15 if Periode=='15min' else 5 if Periode=='5min' else 1
        TsBougieCouranteDt =  datetime.datetime.strptime(bar.date, '%Y%m%d %H:%M:%S')
        TsBougieNextDt1     = TsBougieCouranteDt + datetime.timedelta(minutes=intPeriode)
        TsBougieNextDt2     = TsBougieCouranteDt + datetime.timedelta(minutes=intPeriode*2)
        # self.bot.cpt.loc[F,['BougieClotured']] = True
        self.bot.cpt.loc[F,['UpdateStarted']]  = True
        
        # print(self.bot.cpt)

        self.bot.on_bar_clotured(reqId, bar, TsReqDt)

        self.bot.cpt.loc[F,['TsClotureNextBougieStr']] = TsBougieNextDt2.strftime('%Y%m%d %H:%M:%S')
        self.bot.cpt.loc[F,['TsClotureBougieCouranteStr']] = TsBougieNextDt1.strftime('%Y%m%d %H:%M:%S')

                

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        #super().historicalDataEnd(reqId, start, end)
        print(threading.currentThread().getName() , "Mock Fin réception flux - HistoricalDataEnd - ReqId:", reqId, "from", start, "to", end)


        # F=(self.bot.cpt['reqId'] == reqId)
        
        # F2=(self.df_15m_5m_1m['reqId']==reqId)
        # l = self.df_15m_5m_1m.loc[F2]
        
        # self.bot.cpt.loc[F,'date'] = l[.date
        # self.bot.cpt.loc[F,'bar'] = bar


        
        self.bot.on_historicalDataEnd(reqId)

    def GenererEvtsBougies(self, TsReqDt):
        #super().historicalDataEnd(reqId, start, end)
        ts = datetime.datetime.today()
        print(" "*20)
        print("---"*60)
        print(ts, threading.currentThread().getName() , "Mock GenererEvtsBougies pour :", TsReqDt)
        print("---"*60)
        
        TsReqStr = TsReqDt.strftime("%Y-%m-%d %H:%M:%S")
        
        F=(self.df_15m_5m_1m['TsAffichageDt']==TsReqDt)
        print(self.df_15m_5m_1m.loc[F])
        
        b = BarData()

        for i in self.df_15m_5m_1m.loc[F].index:
            print(i)

            iLigne = self.df_15m_5m_1m.loc[i]

            iReqId = iLigne['reqId']
            
            # print(ts, threading.currentThread().getName(),"Ligne lue :")
            # print(self.df_15m_5m_1m.loc[i, ['Ts']])
            d = iLigne['Ts']
            # print("type date:",d.dtype)
            # print("date:",d)
            # print("iReqId:",iReqId)
            b.date     = d.strftime("%Y%m%d %H:%M:%S")
            # print("bar.date:",b.date)
            b.open     = iLigne['open']
            b.high     = iLigne['high']
            b.low      = iLigne['low']
            b.close    = iLigne['close']
            b.volume   = iLigne['Volume']
            b.barCount = 0
            b.average  = 0     
            # print(b)
            self.historicalDataUpdate(iReqId, b, TsReqDt)
                
 
                
    def maj_Histo_ohlc(self,reqId):
        
        ts = datetime.datetime.today()
        F_reqId=(self.listeReq['reqID']==reqId)
        iReq = self.listeReq.loc[F_reqId].index.values[0]
        # print("iReq:",iReq)
        TailleBougie = self.listeReq.loc[iReq,['TailleBougies']].values[0]
        iContrat = self.listeReq.loc[iReq,['Contrat']].values[0]        
        print(ts, threading.currentThread().getName(), "maj_Histo_ohlc " , TailleBougie, reqId)
        
        TS_Str =  self.listeReq.loc[iReq,['DateFin']].values[0]
        # print("TS_Str:"+TS_Str)
        DateStr = TS_Str[:4] + '-' + TS_Str[4:6] + '-' + TS_Str[6:8]

        DateFinDt = datetime.datetime.strptime(TS_Str, '%Y%m%d %H:%M:%S')     
        
        ProfHisto = self.listeReq.loc[iReq,['ProfHisto']].values[0]
        UniteProfHisto = ProfHisto[-1]
        print("UniteProfHisto:",UniteProfHisto)
        ValeurProfHisto = int(ProfHisto[:-2])
        if UniteProfHisto=="D":
            DateDebDt = DateFinDt +  datetime.timedelta(days=-ValeurProfHisto)
        elif UniteProfHisto=="M":
            DateDebDt = DateFinDt +  datetime.timedelta(minutes=-ValeurProfHisto)
        elif UniteProfHisto=="S":
            DateDebDt = DateFinDt +  datetime.timedelta(seconds=-ValeurProfHisto)
        else:
            DateDebDt = DateFinDt +  datetime.timedelta(days=-1)
            print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - ProfHisto invalide : " + ProfHisto)
        print("DateDebDt:",DateDebDt) 

        if TailleBougie == '1 day':
        
            # print("DateStr:"+DateStr)
            FicHLCS = repHisto_1D + '\\HistoHLCS_Réajustés_pour_le_' + DateStr + '.csv'
            ts = datetime.datetime.today()
            print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - Chargement fichier  " + FicHLCS)
            
    
            try:
                with open(FicHLCS): 
                    #print("GetHisto = False")
                    FICOK = True
            except IOError:
                    print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - Fichier inexistant : ", FicHLCS)
                    quit()
     
            dfJ = pd.read_csv(FicHLCS, sep=';',decimal='.',parse_dates=['Date'])
            dfJ.sort_values(by=['Contrat','Date'], ascending=True, inplace=True)        
            dic= {'YM':'DOW-mini', 'NQ':'NASDAQ-mini', 'DXM':'DAX-mini'}
            
            ContratLib = dic[self.listeReq.loc[iReq,['Contrat']].values[0]]
            F=(dfJ['Contrat']==ContratLib)
            dfJc = dfJ.loc[F]
    
            datefinDepassed = False           
            b = BarData()
     
            for i in dfJc.index:
    
                 if datetime.datetime.date(dfJc.loc[i,['Date']].values[0]) < datetime.datetime.date(DateFinDt) and \
                    datetime.datetime.date(dfJc.loc[i,['Date']].values[0]) > datetime.datetime.date(DateDebDt)  :
    
    
                     # print(ts, threading.currentThread().getName(),"Ligne lue :")
                     # print(dfJc.loc[i])
                     b.date     = dfJc.loc[i,['Date']].values[0].strftime("%Y%m%d")
                     b.open     = 0
                     b.high     = dfJc.loc[i,['highJ']].values[0]
                     b.low      = dfJc.loc[i,['lowJ']].values[0]
                     b.close    = dfJc.loc[i,['settleJ']].values[0]
                     b.volume   = dfJ.loc[i,['volJ']].values[0]
                     b.barCount = 0
                     b.average  = 0     
                     
                     self.historicalData(reqId, b)
                 
                 if datetime.datetime.date(dfJc.loc[i,['Date']].values[0]) >= datetime.datetime.date(DateFinDt) and datefinDepassed==False:
                     self.listeReq.loc[iReq,['lastIndexTraited']]=i
                     datefinDepassed = True
    
    
            # self.historicalDataEnd(reqId,"start","end")
                     
        Add_Minutes= 0                         
        
        # Add_Minutes = TailleBougie.map({'15mins':15, '5mins':5,'1min':1} )
        Add_Minutes = 15 if TailleBougie == '15 mins' else 5 if TailleBougie == '5 mins' else 1
        # Str_Minutes = TailleBougie.map({'15mins':'15min', '5mins':'5min','1min':'1min'} )
        Str_Minutes = '15min' if TailleBougie == '15 mins' else '5min' if TailleBougie == '5 mins' else '1min'
        TS_Str =  self.listeReq.loc[iReq,['DateFin']].values[0]
        DateStr = TS_Str[:4] + '-' + TS_Str[4:6] + '-' + TS_Str[6:8]
        repMois  = "\\" + DateStr[0:4] + "-M" + DateStr[5:7]
        FicHLCS = repHisto_15m_5m_1m  + repMois + '\\HistoBars_' + Str_Minutes + '_Réajustés_pour_le ' + DateStr + '.csv'
        ts = datetime.datetime.today()
        print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - Chargement fichier  " + FicHLCS)
    
    
        if TailleBougie in ['15 mins', '5 mins', '1 min']:
    
    
            # DateFinDt = datetime.datetime.strptime(TS_Str, '%Y%m%d %H:%M:%S')                
            
            
            try:
                with open(FicHLCS): 
                    #print("GetHisto = False")
                    FICOK = True
            except IOError:
                    print(ts, threading.currentThread().getName(), "Fichier inexistant : ", FicHLCS)
                    quit()
     
            dfHc1 = pd.read_csv(FicHLCS, sep=';',decimal='.',parse_dates=['Ts'])
            print(ts, threading.currentThread().getName(), "Fichier chargé, nb lignes = ", len(dfHc1))
            dfHc1.sort_values(by=['Contrat','Ts'], ascending=True, inplace=True)   
            F=(dfHc1['Contrat']==iContrat)
            dfHc=dfHc1.loc[F].copy()
            print(ts, threading.currentThread().getName(), "dfHc copié, nb lignes = ", len(dfHc))
            dfHc['reqId']=reqId
            dfHc['TsAffichageDt'] = dfHc['Ts'].apply(lambda x: x + datetime.timedelta(minutes=Add_Minutes)  )
            self.df_15m_5m_1m = pd.concat([dfHc, self.df_15m_5m_1m])
            print(ts, threading.currentThread().getName(), "df_15m_5m_1m concaténé, nb lignes = ", len(self.df_15m_5m_1m))
            
            dfHc.reset_index(inplace=True)
            dfHc.drop(columns=['index'],inplace=True)   
            self.df_15m_5m_1m.reset_index(inplace=True)
            self.df_15m_5m_1m.drop(columns=['index'],inplace=True)   
     
            F_Fin=(dfHc['TsAffichageDt']<DateFinDt)
            F_Deb=(dfHc['TsAffichageDt']>=DateDebDt)
            df_copy1 = dfHc.loc[F_Fin & F_Deb].copy()
            df_copy = df_copy1[['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50','EMA05','RSI14','xopen','xclose','xlow','xhigh','xcouleur']].copy()
    
            if TailleBougie == '15 mins':
                self.bot.Histo_ohlc_15min = pd.concat([df_copy, self.bot.Histo_ohlc_15min])
                self.bot.Histo_ohlc_15min.reset_index(inplace=True)
                self.bot.Histo_ohlc_15min.drop(columns=['index'],inplace=True)   
                self.bot.i_15min=self.bot.i_15min + len(df_copy)
                self.bot.cpt.loc[F,'Contrat']
                
            if TailleBougie == '5 mins':
                self.bot.Histo_ohlc_5min = pd.concat([df_copy, self.bot.Histo_ohlc_5min])
                self.bot.Histo_ohlc_5min.reset_index(inplace=True)
                self.bot.Histo_ohlc_5min.drop(columns=['index'],inplace=True)   
                self.bot.i_5min=self.bot.i_5min + len(df_copy)
            if TailleBougie == '1 min':
                self.bot.Histo_ohlc_1min = pd.concat([df_copy, self.bot.Histo_ohlc_1min])
                self.bot.Histo_ohlc_1min.reset_index(inplace=True)
                self.bot.Histo_ohlc_1min.drop(columns=['index'],inplace=True)   
                self.bot.i_1min=self.bot.i_1min + len(df_copy)
                
               

            F_req=(self.bot.cpt['reqId'] == reqId)
            ll=df_copy.iloc[-1]
            self.bot.cpt.loc[F_req,'TsBougieCouranteStr']=ll['Ts'].strftime('%Y%m%d %H:%M:%S')
            # self.bot.cpt.loc[F_req,'bar']=bar
    

            # Bougies Heiken Ashi - Daily

            def HeikenAshi(dataframe):
            
                df = dataframe.copy()
            
                df['xclose']=(df.open + df.high + df.low + df.close)/4
            
                df.reset_index(inplace=True)
            
                xopen = [ (df.open[0] + df.close[0]) / 2 ]
                [ xopen.append((xopen[i] + df.xclose.values[i]) / 2) \
                for i in range(0, len(df)-1) ]
                df['xopen'] = xopen
            
                df.set_index('index', inplace=True)
            
                df['xhigh']=df[['xopen','xclose','high']].max(axis=1)
                df['xlow']=df[['xopen','xclose','low']].min(axis=1)

                xdiff = df['xclose'] - df['xopen']
                xcouleur = xdiff.apply(lambda x: 'Green' if x > 2 else 'Red' if x < -2 else 'Flat')
                df['xcouleur'] = xcouleur

                return df[['xopen','xclose','xlow','xhigh','xcouleur']]

            # Bougies Heiken Ashi - Daily
            
            i_contrat = self.bot.cpt.loc[F_req,'Contrat'].values[0]

            if TailleBougie == '1 day':
 
                ts = datetime.datetime.today()
                print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - DEB Calcul HA daily...  " )

                F_Contrat = (self.bot.Histo_ohlc_day['Contrat']==i_contrat)

                self.bot.Histo_ohlc_day.loc[F_Contrat,['xopen','xclose','xlow','xhigh','xcouleur']] = HeikenAshi(self.bot.Histo_ohlc_day.loc[F_Contrat])

 
                ts = datetime.datetime.today()
                print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - FIN Calcul HA daily...  ", len(self.bot.Histo_ohlc_day.loc[F_Contrat]) )
    
            # Bougies Heiken Ashi - 15min
            if TailleBougie == '15 mins':
                ts = datetime.datetime.today()
                print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - DEB Calcul HA 15 mins...  " )

                F_Contrat = (self.bot.Histo_ohlc_15min['Contrat']==i_contrat)

                self.bot.Histo_ohlc_15min.loc[F_Contrat,['xopen','xclose','xlow','xhigh','xcouleur']] = HeikenAshi(self.bot.Histo_ohlc_15min.loc[F_Contrat])

    
                ts = datetime.datetime.today()
                print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - FIN Calcul HA 15 mins...  " , len(self.bot.Histo_ohlc_15min.loc[F_Contrat]))

            # Bougies Heiken Ashi - 5min
            if TailleBougie == '5 mins':
                
                ts = datetime.datetime.today()
                print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - DEB Calcul HA 5min...  " )

                F_Contrat = (self.bot.Histo_ohlc_5min['Contrat']==i_contrat)

                self.bot.Histo_ohlc_5min.loc[F_Contrat,['xopen','xclose','xlow','xhigh','xcouleur']] = HeikenAshi(self.bot.Histo_ohlc_5min.loc[F_Contrat])

    
                ts = datetime.datetime.today()
                print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - FIN Calcul HA 5min...  " , len(self.bot.Histo_ohlc_5min.loc[F_Contrat]))

            # Bougies Heiken Ashi - 1min
            if TailleBougie == '1 min':

                ts = datetime.datetime.today()
                print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - DEB Calcul HA 1 min...  " )

                F_Contrat = (self.bot.Histo_ohlc_1min['Contrat']==i_contrat)

                self.bot.Histo_ohlc_1min.loc[F_Contrat,['xopen','xclose','xlow','xhigh','xcouleur']] = HeikenAshi(self.bot.Histo_ohlc_1min.loc[F_Contrat])

    
                ts = datetime.datetime.today()
                print(ts, threading.currentThread().getName(), "maj_Histo_ohlc - FIN Calcul HA 1 min...  ", len(self.bot.Histo_ohlc_1min.loc[F_Contrat]) )
                
    
            #Pour reprendre au bon endroit sur la suite du flux :
            # self.listeReq.loc[iReq,['lastIndexTraited']]=i
            datefinDepassed = True

            # F=(self.bot.cpt['reqId'] == reqId)
            # # self.bot.cpt.loc[F,'i']=self.cpt.loc[F,'i']+1
            # # i_cpt =  self.bot.cpt.loc[F,'i'].values[0]
            # self.bot.cpt.loc[F,'TsBougieCouranteStr']=bar.date
            # self.bot.cpt.loc[F,'bar']=bar
    



   
 
    def run(self):
    
         
        
        print('==================================================================================')
        print(threading.currentThread().getName(),'  Package Mock_Api_IB.py - Mock_IBApi.run ')
        print('==================================================================================')
        print(self.listeReq)

        self.df_15m_5m_1m = pd.DataFrame()
        self.df_15m_5m_1m = None                
            
        ts = datetime.datetime.today()

        while len(self.listeReq) < 12:
            print(ts, threading.currentThread().getName(), "run : attente réception requetes ib, nb=",len(self.listeReq))
            tm.sleep(1)
        
        print(ts, threading.currentThread().getName(), "Liste des requetes demandées :")
        print(self.listeReq)
        
        for iReq in self.listeReq.index:
        
            ts = datetime.datetime.today()

            iReqId =  self.listeReq.loc[iReq,['reqID']].values[0]   
            print("")
            print("---"*20)
            print(ts, threading.currentThread().getName(), "run_mock - Traitement requete ", iReqId)
            # print(self.listeReq)

            self.maj_Histo_ohlc(iReqId)
            
            # print("----+++"*20)
            # print("self.bot.Histo_ohlc_15min")
            # print(self.bot.Histo_ohlc_15min)            
            
            # self.bot.on_bar_clotured(iReqId, )

                     
            ts = datetime.datetime.today()
            print(ts, threading.currentThread().getName(), "Appel historicalDataEnd")
            self.historicalDataEnd(iReqId,"start","end")


            #quit()
            
            
        
        # print('')
        # print("----"*20)
        # print(threading.currentThread().getName(),'  Mock Run - Affichage des donnéeshistorisées... ')
        # print("----"*20)
        # print("self.bot.Histo_ohlc_day")
        # print(self.bot.Histo_ohlc_day)
        # print("----"*20)
        # print("self.bot.Histo_ohlc_15min")
        # print(self.bot.Histo_ohlc_15min)
        # print("----"*20)
        # print("self.bot.Histo_ohlc_5min")
        # print(self.bot.Histo_ohlc_5min)
        # print("----"*20)
        # print("self.bot.Histo_ohlc_1min")
        # print(self.bot.Histo_ohlc_1min)
        # print('')
            
        print('')
        print("----"*20)
        print(threading.currentThread().getName(),'  Mock Run - Début du pseudo temps réel... ')
        print("----"*20)
        print('')
       
        self.df_15m_5m_1m.reset_index(inplace=True)
        self.df_15m_5m_1m.drop(columns=['index'],inplace=True)        
        
        GoTrt = True
        NextTsDt = datetime.datetime.strptime(Mock_TS_Cour, '%Y-%m-%d %H:%M:%S') #+ datetime.timedelta(minutes=1)  

        self.bot.cpt['BougieClotured'] = False
        
        NextTsDt1min   = NextTsDt
        NextTsDt5min   = NextTsDt + datetime.timedelta(minutes=4)  
        NextTsDt15min  = NextTsDt + datetime.timedelta(minutes=14)  
        F=(self.bot.cpt['Periode'] == '15min')
        self.bot.cpt.loc[F,['TsClotureBougieCouranteStr']] = NextTsDt15min.strftime('%Y%m%d %H:%M:%S')
        self.bot.cpt.loc[F,['TsClotureNextBougieStr']] = NextTsDt15min.strftime('%Y%m%d %H:%M:%S')
        F=(self.bot.cpt['Periode'] == '5min')
        self.bot.cpt.loc[F,['TsClotureBougieCouranteStr']] = NextTsDt5min.strftime('%Y%m%d %H:%M:%S')
        self.bot.cpt.loc[F,['TsClotureNextBougieStr']] = NextTsDt5min.strftime('%Y%m%d %H:%M:%S')
        F=(self.bot.cpt['Periode'] == '1min')
        self.bot.cpt.loc[F,['TsClotureBougieCouranteStr']] = NextTsDt1min.strftime('%Y%m%d %H:%M:%S')
        self.bot.cpt.loc[F,['TsClotureNextBougieStr']] = NextTsDt1min.strftime('%Y%m%d %H:%M:%S')


        print("****"*20)
        print(" self.cpt:")
        print("****"*20)
        print( self.bot.cpt)
        print("****"*20)
        print(" self.listeReq:")
        print(self.listeReq)
        print("****"*20)


        while GoTrt:
            print(GoTrt)
            ts = datetime.datetime.today()
            print(ts, threading.currentThread().getName(),'  Mock Run - Appel GenererEvtsBougies pour ts=',NextTsDt)
            self.GenererEvtsBougies(NextTsDt)
            tm.sleep(NbSecWaitPour1minReelle)
            NextTsDt = NextTsDt + datetime.timedelta(minutes=1)  
            

             
            
        print(threading.currentThread().getName(),'  Package Mock_Api_IB.py - Mock_IBApi.run - Fin traitement... ')
        print(self.listeReq)

                    
                
                  
            
            

        
class Object(object):
    def __str__(self):
        return "Object"
        
        
class Contract(Object):
    def __init__(self):
        self.conId = 0
        self.symbol = ""
        self.secType = ""
        self.lastTradeDateOrContractMonth = ""
        self.strike = 0.  # float !!
        self.right = ""
        self.multiplier = ""
        self.exchange = ""
        self.primaryExchange = "" # pick an actual (ie non-aggregate) exchange that the contract trades on.  DO NOT SET TO SMART.
        self.currency = ""
        self.localSymbol = ""
        self.tradingClass = ""
        self.includeExpired = False
        self.secIdType = ""	  # CUSIP;SEDOL;ISIN;RIC
        self.secId = ""
        self.nomContrat = ""

        #combos
        self.comboLegsDescrip = ""  # type: str; received in open order 14 and up for all combos
        self.comboLegs = None     # type: list<ComboLeg>
        self.deltaNeutralContract = None
        
class BarData(Object):
    def __init__(self):
        self.date = ""
        self.open = 0.
        self.high = 0.
        self.low = 0.
        self.close = 0.
        self.volume = 0
        self.barCount = 0
        self.average = 0.

    def __str__(self):
        return "Date: %s, Open: %f, High: %f, Low: %f, Close: %f, Volume: %d, Average: %f, BarCount: %d" % (self.date, self.open, self.high,
            self.low, self.close, self.volume, self.average, self.barCount)
