"""
Copyright (C) 2019 Interactive Brokers LLC. All rights reserved. This code is subject to the terms
 and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable.
"""


import datetime

HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

DateinDebStr="2022-09-15"
DateInFinStr = HierStr
# DateInFinStr="2022-01-21"

portProd = 7496
portSimu = 7497
portTWS=portSimu
 

# NbTicksDemandes : nb de ticks à chaque requete. Max autorisé par l'API = 1000
# 1 tick = 1 transaction, qui peut concerner plusieurs lots
# l'API peut rendre plus de ticks que 1000, car elle rend systématiquement tous les ticks de chaque seconde
# donc s'il y a eu déjà 95 ticks entre 15:30:00 et 15:45:15, et qu'il y a 15 ticks à l'heure précise 15:45:16,
# l'API va rendre n tout 95+15=110 ticks de 15:30:00 à 15:45:16
# Par contre si sur la dernière seconde on a + de 100 ticks, l'API va perdre les pédales, il semble que si on dépasse
# au total 1100 ticks elle ne renvoie plus rien...
# donc dans ce cas, pour s'en sortir, il faut réduire la taile des paquets demandés, et réduire par exemple à 900 ticks
NbTicksDemandes = 900

ListeContratsIn = [["NASDAQ-mini",["202212","202303"]],
                   ["DOW-mini"   ,["202212","202303"]],
                   ["DAX-mini"   ,["202212","202303"]]
                 ]

ListeContrats = []
for i in ListeContratsIn:
    for j in i[1]:
        ListeContrats.append([i[0] , j])

print(ListeContrats)
contrat_courant=0
Future_NomContrat=ListeContrats[contrat_courant][0]
Future_EcheanceContrat=ListeContrats[contrat_courant][1]

import argparse
import datetime
import collections
import datetime
import inspect
import logging
import os.path
import time as tm
from math import pi

#import Niveaux, Horaires
import pandas as pd
from bokeh.io import export_png
from bokeh.models import (DatetimeTickFormatter, DaysTicker, FuncTickFormatter,
                          LinearAxis, Range1d)
from bokeh.models.tickers import FixedTicker
from bokeh.plotting import figure, output_file, save, show
from ContractSamples import ContractSamples
from ibapi import utils, wrapper
from ibapi.account_summary_tags import *
from ibapi.client import EClient
from ibapi.commission_report import CommissionReport
# types
from ibapi.common import *  # @UnusedWildImport
from ibapi.contract import *  # @UnusedWildImport
from ibapi.execution import Execution, ExecutionFilter
from ibapi.order import *  # @UnusedWildImport
from ibapi.order_condition import *  # @UnusedWildImport
from ibapi.order_state import *  # @UnusedWildImport
from ibapi.tag_value import TagValue
from ibapi.ticktype import *  # @UnusedWildImport
from ibapi.utils import iswrapper

#from OrderSamples import OrderSamples
#from AvailableAlgoParams import AvailableAlgoParams
#from ScannerSubscriptionSamples import ScannerSubscriptionSamples
#from FaAllocationSamples import FaAllocationSamples
#from ibapi.scanner import ScanData



#DateStr = datetime.datetime.today().strftime("%Y-%m-%d")


#DateStr = DateinDebStr 
#DateStr="2021-04-22"

DateDebDt=datetime.datetime.strptime(DateinDebStr, '%Y-%m-%d')
DateFinDt=datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d')
DateCourDt = DateDebDt
DateStr = DateCourDt.strftime('%Y-%m-%d')

#DateCourDt = DateCourDt + datetime.timedelta(days=1)
path='Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\00 - Histo Ticks\\HistoTicks_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat +'\\HistoTicks_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat + "_Q" 
FichierHistoTicksJour    =   path + DateStr + '.csv'
TsDebutStr = DateStr[0:4] + DateStr[5:7] + DateStr[8:10] + " 00:00:00"
TsFinStr   = DateStr[0:4] + DateStr[5:7] + DateStr[8:10] + " 23:59:59"
ts_fin_du_jourG = datetime.datetime.strptime(TsFinStr,  '%Y%m%d  %H:%M:%S')


 
GetHistoTicks = True
 
def SetupLogger():
    if not os.path.exists("log"):
        os.makedirs("log")

    tm.strftime("pyibapi.%Y%m%d_%H%M%S.log")

    recfmt = '(%(threadName)s) %(asctime)s.%(msecs)03d %(levelname)s %(filename)s:%(lineno)d %(message)s'

    timefmt = '%y%m%d_%H:%M:%S'

    # logging.basicConfig( level=logging.DEBUG,
    #                    format=recfmt, datefmt=timefmt)
    logging.basicConfig(filename=tm.strftime("log/pyibapi.%y%m%d_%H%M%S.log"),
                        filemode="w",
                        level=logging.INFO,
                        format=recfmt, datefmt=timefmt)
    logger = logging.getLogger()
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    logger.addHandler(console)


def printWhenExecuting(fn):
    def fn2(self):
        print("   doing", fn.__name__)
        fn(self)
        print("   done w/", fn.__name__)

    return fn2

def printinstance(inst:Object):
    attrs = vars(inst)
    print(', '.join("%s: %s" % item for item in attrs.items()))

class Activity(Object):
    def __init__(self, reqMsgId, ansMsgId, ansEndMsgId, reqId):
        self.reqMsdId = reqMsgId
        self.ansMsgId = ansMsgId
        self.ansEndMsgId = ansEndMsgId
        self.reqId = reqId


class RequestMgr(Object):
    def __init__(self):
        # I will keep this simple even if slower for now: only one list of
        # requests finding will be done by linear search
        self.requests = []

    def addReq(self, req):
        self.requests.append(req)

    def receivedMsg(self, msg):
        pass


# ! [socket_declare]
class TestClient(EClient):

    def __init__(self, wrapper):
        EClient.__init__(self, wrapper)
        # ! [socket_declare]

        # how many times a method is called to see test coverage
        self.clntMeth2callCount = collections.defaultdict(int)
        self.clntMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nReq = collections.defaultdict(int)
        self.setupDetectReqId()

    def countReqId(self, methName, fn):
        def countReqId_(*args, **kwargs):
            self.clntMeth2callCount[methName] += 1
            idx = self.clntMeth2reqIdIdx[methName]
            if idx >= 0:
                sign = -1 if 'cancel' in methName else 1
                self.reqId2nReq[sign * args[idx]] += 1
            return fn(*args, **kwargs)

        return countReqId_

    def setupDetectReqId(self):

        methods = inspect.getmembers(EClient, inspect.isfunction)
        for (methName, meth) in methods:
            if methName != "send_msg":
                # don't screw up the nice automated logging in the send_msg()
                self.clntMeth2callCount[methName] = 0
                # logging.debug("meth %s", name)
                sig = inspect.signature(meth)
                for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                    (paramName, param) = pnameNparam # @UnusedVariable
                    if paramName == "reqId":
                        self.clntMeth2reqIdIdx[methName] = idx

                setattr(TestClient, methName, self.countReqId(methName, meth))

                # print("TestClient.clntMeth2reqIdIdx", self.clntMeth2reqIdIdx)


# ! [ewrapperimpl]
class TestWrapper(wrapper.EWrapper):
    # ! [ewrapperimpl]
    def __init__(self):
        wrapper.EWrapper.__init__(self)

        self.wrapMeth2callCount = collections.defaultdict(int)
        self.wrapMeth2reqIdIdx = collections.defaultdict(lambda: -1)
        self.reqId2nAns = collections.defaultdict(int)
        self.setupDetectWrapperReqId()

    # TODO: see how to factor this out !!

    def countWrapReqId(self, methName, fn):
        def countWrapReqId_(*args, **kwargs):
            self.wrapMeth2callCount[methName] += 1
            idx = self.wrapMeth2reqIdIdx[methName]
            if idx >= 0:
                self.reqId2nAns[args[idx]] += 1
            return fn(*args, **kwargs)

        return countWrapReqId_

    def setupDetectWrapperReqId(self):

        methods = inspect.getmembers(wrapper.EWrapper, inspect.isfunction)
        for (methName, meth) in methods:
            self.wrapMeth2callCount[methName] = 0
            # logging.debug("meth %s", name)
            sig = inspect.signature(meth)
            for (idx, pnameNparam) in enumerate(sig.parameters.items()):
                (paramName, param) = pnameNparam # @UnusedVariable
                # we want to count the errors as 'error' not 'answer'
                if 'error' not in methName and paramName == "reqId":
                    self.wrapMeth2reqIdIdx[methName] = idx

            setattr(TestWrapper, methName, self.countWrapReqId(methName, meth))

            # print("TestClient.wrapMeth2reqIdIdx", self.wrapMeth2reqIdIdx)


# this is here for documentation generation
"""
#! [ereader]
        # You don't need to run this in your code!
        self.reader = reader.EReader(self.conn, self.msg_queue)
        self.reader.start()   # start thread
#! [ereader]
"""

# ! [socket_init]
class TestApp(TestWrapper, TestClient):

    def __init__(self):
        TestWrapper.__init__(self)
        TestClient.__init__(self, wrapper=self)
        # ! [socket_init]
        self.nKeybInt = 0
        self.started = False
        self.nextValidOrderId = None
        self.permId2ord = {}
        self.reqId2nErr = collections.defaultdict(int)
        self.globalCancelOnly = False
        self.simplePlaceOid = None
        self.reqID=0
        self.contract=None
        self.reqTickByTickData_ID = None
        self.numPasTicks = 0
        self.numTick = 0
        self.demande_open_orders=False
        self.NiveauDuJour = None
        self.NiveauxATrader = None
        self.NivAdj = None
        self.nom_niveau_sup = None
        self.nom_niveau_inf = None
        self.niveau_sup     = None
        self.niveau_inf     = None
        self.niveau_trading_sup = None		
        self.niveau_trading_inf = None		
        self.ecart_niveau_sup = None
        self.ecart_niveau_inf = None
        self.prix_prec    = None
        self.prix_courant = None
        self.Histo_Croisements_Niveaux = None
        self.Histo_Croisements_Triggers_H = None
        self.Histo_Croisements_Triggers_L = None
        self.NiveauRisquePeriodePrec     = None
        self.NiveauRisquePeriodeCourante = None   
        self.NiveauRisqueVolumesActuels = "OK"   
        self.NiveauRisqueVolumesPrec    = "OK"   
        self.CalculDebit_TsCour = None
        self.CalculDebit_TsPrec = None
        self.DebitCourant = 0 #Dernier débit calculé
        #self.ordres_lies = pd.DataFrame(columns=['ID_Lie_1','ID_Lie_2'])
        self.debutReceptionFlux5min = True
        self.NbPeriodesDebitOK = 0

        # Variables pour BilanDuJour
        self.Histo_ticks = pd.DataFrame(columns=['NumTrade','ts','Prix','NbLots','CumulNbLots'])
        self.Histo_ticks.set_index("NumTrade", inplace=True)
        self.nbTicks  = 0
        self.nbTrades = 0
        self.time_dernier_tick = None
        self.ts_next = TsDebutStr
        self.nbTicksPrec = 2

        # Paramètres ajustables
        self.stop_loss_S1   = 15
        self.stop_profit_S1 = 3
        self.nb_ticks_minimum_avant_reactivation_niveau = 1000
        self.duree_minimum_avant_reactivation_niveau    = 120
        self.seuilFusionNiveaux = 10 # ecart de points sous lesquel on fusionnera 2 niveaux trop proches
        self.ParamDureePeriodecalculDebit = 50   # Nb ticks à prendre en compte pour réévaluation si période est risquée du point de vue du débit
        self.ParamDebitTicksMax = 3.5   #débit max en nbTicks / sec
        self.ParamNbPeriodeDebitOK = 3   # Après une periode de debit trop elevé, combien faut-il avoir de periodes calmes pour réctiver le trading ?
        self.ParamDistanceTriggers = 10 # distance des triggers par rapport à leur niveau de référence
        self.DureeVieOrdreParent_Ticks = 1000 #si l'ordre positionné il y a 1000 ticks n'a toujours pas été activé, c'est probablement qu'on est en train de ranger juste en dessous, c'est dangereux, on risque le breakOut
        self.FrequenceVerificationSituationRange = 50 # verif tous les 50 ticks    

        #GetHisto
        self.DateCourDt = DateCourDt
        self.DateStr    = DateStr
        self.DateDebDt  = DateDebDt
        self.DateFinDt  = DateFinDt
        
        self.FichierHistoTicksJour    =   FichierHistoTicksJour
        self.TsDebutStr = TsDebutStr
        self.TsFinStr   = TsFinStr
        self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')
        self.tsPrec_dt =  None
        self.ts_dt =  None
        
        self.contrat_courant = contrat_courant
        self.ListeContrats   = ListeContrats
        



    def dumpTestCoverageSituation(self):
        for clntMeth in sorted(self.clntMeth2callCount.keys()):
            logging.debug("ClntMeth: %-30s %6d" % (clntMeth,
                                                   self.clntMeth2callCount[clntMeth]))

        for wrapMeth in sorted(self.wrapMeth2callCount.keys()):
            logging.debug("WrapMeth: %-30s %6d" % (wrapMeth,
                                                   self.wrapMeth2callCount[wrapMeth]))

    def dumpReqAnsErrSituation(self):
        logging.debug("%s\t%s\t%s\t%s" % ("ReqId", "#Req", "#Ans", "#Err"))
        for reqId in sorted(self.reqId2nReq.keys()):
            nReq = self.reqId2nReq.get(reqId, 0)
            nAns = self.reqId2nAns.get(reqId, 0)
            nErr = self.reqId2nErr.get(reqId, 0)
            logging.debug("%d\t%d\t%s\t%d" % (reqId, nReq, nAns, nErr))

    @iswrapper
    # ! [connectack]
    def connectAck(self):
        if self.asynchronous:
            self.startApi()

    # ! [connectack]

    @iswrapper
    # ! [nextvalidid]
    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)

        logging.debug("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId
    # ! [nextvalidid]

        # we can start now
        self.start()

    def start(self):
        if self.started:
            return

        self.started = True
        reqId = 18000
        self.contrat_courant=-1
        JourneeDejaTraiteePourCeContrat = True
        #Passage au contrat suivant si pas tous déjà faits :
        while JourneeDejaTraiteePourCeContrat == True and self.contrat_courant < len(self.ListeContrats) - 1:
            self.contrat_courant = self.contrat_courant + 1
            Future_NomContrat=self.ListeContrats[self.contrat_courant][0]
            Future_EcheanceContrat=self.ListeContrats[self.contrat_courant][1]
            print("----------------------")
            print("Start - Contrat suivant : " + Future_NomContrat + "- Ech"+ Future_EcheanceContrat)
            print("----------------------")
            self.contract = self.create_contract(Future_NomContrat, Future_EcheanceContrat)  # Create a contract
 
            
            repertoire = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\00 - Histo Ticks\\HistoTicks_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat
            path = repertoire + '\\HistoTicks_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat + "_Q"

            if not os.path.exists(repertoire):
                os.makedirs(repertoire)

            self.DateStr    = DateStr
            self.DateDebDt  = DateDebDt
            self.DateFinDt  = DateFinDt
            
            self.tsPrec_dt =  None
            self.ts_dt =  None

            self.DateCourDt =  self.DateDebDt
            self.FichierHistoTicksJour    =   path + self.DateStr + '.csv'
            self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
            self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
            self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')
 
            self.nbTicks = 0
            self.nbTrades = 0
            self.Histo_ticks = self.Histo_ticks.drop(self.Histo_ticks.index)
            print('===========4 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
            #print(self.Histo_ticks)

            #Si fichier déjà existant, on passe à la journée suivante :
            try:
                with open(self.FichierHistoTicksJour): 
                    print('Journée déjà traitée:', self.DateStr)
                    JourneeDejaTraiteePourCeContrat = True
            except IOError:
                    JourneeDejaTraiteePourCeContrat = False

            #Recherche Journée pas déjà traitée, passage au lendemain:
            while JourneeDejaTraiteePourCeContrat == True and self.DateCourDt < self.DateFinDt:
                    
                self.DateCourDt = self.DateCourDt + datetime.timedelta(days=1)
                self.DateStr = self.DateCourDt.strftime('%Y-%m-%d')
                self.FichierHistoTicksJour    =   path + self.DateStr + '.csv'
                self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
                self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
                self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')

                #Si fichier déjà existant, on passe à la journée suivante :
                try:
                    with open(self.FichierHistoTicksJour): 
                        print('Journée déjà traitée:', self.DateStr)
                        JourneeDejaTraiteePourCeContrat = True
                except IOError:
                    JourneeDejaTraiteePourCeContrat = False

        if JourneeDejaTraiteePourCeContrat == False:
            self.nbTicks = 0
            self.nbTrades = 0
            self.Histo_ticks = self.Histo_ticks.drop(self.Histo_ticks.index)
            print('===========3 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
            #print(self.Histo_ticks)
            print('appel   reqHistoricalTicks, reqId:',reqId+1, self.TsDebutStr)
            self.reqHistoricalTicks(reqId+1, self.contract, self.TsDebutStr, "", NbTicksDemandes, "TRADES", 0, True, []);
        else:
            print("fin reception flux de toutes les journées demandées pour tous les contrats...")

    def BilanDuJour(self):
        

        #Création DF agérgé par 100 ticks :
        self.Histo_ticks.reset_index(inplace=True)
        self.Histo_ticks.sort_values(by=['NumTrade'],inplace=True)
        self.Histo_ticks['NumPas100Ticks']=(self.Histo_ticks['NumTrade'] - 1) // 100
        #self.Histo_ticks=self.Histo_ticks.set_index('ts')
    
        by100ticks=self.Histo_ticks.groupby(['NumPas100Ticks']).agg({'ts':['min','max'],
                                                                     'Prix': ['first', 'max', 'min', 'last'],
                                                                     'NbLots':'sum'})
        d={'ts_min':'min_ts', 'ts_max':'max_ts', 'Prix_first':'open100t', 'Prix_min':'low100t', 'Prix_max':'high100t', 'Prix_last':'close100t',  'NbLots_sum':'Volume'} 
        by100ticks.columns = by100ticks.columns.map('_'.join).to_series().map(d) 
        by100ticks = by100ticks.reset_index() 
        by100ticks['duree']=by100ticks['max_ts']-by100ticks['min_ts']
        #by100ticks=by100ticks.set_index('min_ts')
        by100ticks['date']=pd.to_datetime(by100ticks['min_ts'].apply(lambda x: pd.Timestamp(x).strftime('%Y-%m-%d')))
        #by100ticks=by100ticks.set_index('date')
        by100ticks['semaine']=by100ticks['min_ts'].apply(lambda x: pd.Timestamp(x).strftime('A%Y-S%U'))
        by100ticks['mois']=by100ticks['min_ts'].apply(lambda x: pd.Timestamp(x).strftime('A%Y-M%m'))
        by100ticks['debit']=by100ticks['duree'].apply(lambda x: 100 if x.total_seconds()==0 else 100/(x.total_seconds()))
        print(by100ticks)

        #Récupération des numTicks toutes les 30 minuts pour afficher une ligne verticale :
        
        by30min=by100ticks.copy()    
        #by30min['ts_dt']=by30min['min_ts'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S'))
        
        #datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
        #print(by30min.info())
        by30min.set_index('min_ts',inplace=True)
        by30min=by30min.between_time('08:00','22:00')
        #print(by30min)
        by30min=by30min.groupby(pd.Grouper(freq='30min')).agg({'NumPas100Ticks':['first','last']})
        d={'NumPas100Ticks_first':'first_NumPas100Ticks', 'NNumPas100Ticks_last':'last_NumPas100Ticks'} 
        by30min.columns = by30min.columns.map('_'.join).to_series().map(d) 
        by30min = by30min.reset_index() 
        
        print(by30min)

        

        #Récupération des positions de la journée:
        MesPositionsCsv = pd.read_csv(FichierHistoOrdresJour, sep=';')
        F=(MesPositionsCsv['Bracket']=='Parent')
        MesPositions = MesPositionsCsv[F]
        for i in MesPositions.index:
            TS_Ordre=MesPositions.loc[i,'TsExec']
            F1=(by100ticks['min_ts'] <= TS_Ordre)
            F2=(by100ticks['max_ts'] > TS_Ordre)
            N=by100ticks[F1 & F2]
            for j in N.index:
               print(N.loc[j,'NumPas100Ticks'])
               MesPositions.loc[i,'NumPas100Ticks'] = N.loc[j,'NumPas100Ticks']

           # ts_dt = datetime.datetime.fromtimestamp(tick.time)
            
        print(MesPositions)

 
        #Récupération horaires de trading:
        HorairesJour = pd.read_csv(FichierHorairesJour, sep=';', parse_dates=['ts5m'] )
        by5min=by100ticks.copy()    
        by5min.set_index('min_ts',inplace=True)
        by5min=by5min.between_time('08:00','22:00')
        #print(by30min)
        by5min=by5min.groupby(pd.Grouper(freq='5min')).agg({'NumPas100Ticks':['first','last']})
        d={'NumPas100Ticks_first':'first_NumPas100Ticks', 'NumPas100Ticks_last':'last_NumPas100Ticks'} 
        by5min.columns = by5min.columns.map('_'.join).to_series().map(d) 
        by5min = by5min.reset_index() 
        by5min.rename(columns={'min_ts': 'ts5m'}, inplace=True)
        by5min.sort_values(by=['ts5m'], inplace=True)
        HorairesJour.sort_values(by=['ts5m'], inplace=True)
        HorairesJour=pd.merge(HorairesJour,by5min, on="ts5m")
        TsKO=(HorairesJour.NiveauRisque == "KO")
        print(HorairesJour)
        

        #Recupération de variables liées à l'evt :
        ts0=by100ticks.min_ts
        date0=pd.Timestamp(ts0.values[0]).strftime('%A %d %B %Y')
        heure0=pd.Timestamp(ts0.values[0]).strftime('%H:%M:%S')
        
        #Bornes du graphe :
        tsA=ts0+datetime.timedelta(hours=0)
        tsB=ts0+datetime.timedelta(hours=22)
        tsA=tsA.values[0]
        tsB=tsB.values[0]
        by100ticks.set_index('min_ts',inplace=True)
        F1=(by100ticks.index >= tsA)
        F2=(by100ticks.index <= tsB)
        df=by100ticks[F1*F2]
        
        num_deb=df.iloc[0].NumPas100Ticks
        num_fin=df.iloc[-1].NumPas100Ticks
        x_pas=int((num_fin-num_deb)/25)
        y_min=df['low100t'].min()
        y_max=df['high100t'].max()
        y_pas=int((y_max-y_min)/20)
        
        df.reset_index(inplace=True)
        by100ticks.reset_index(inplace=True)
        
        # Recherche des niveaux à mettre dans le graphe :
        #Niveaux = pd.read_csv(NomFichierNiveauxEnrichi, sep=';', dtype={'Prix':np.float64})
        Niveaux = pd.read_csv(NomFichierNiveauxEnrichi, sep=';',decimal=',')
        PrixMax = (Niveaux['Prix'] <= y_max+20)
        PrixMin = (Niveaux['Prix'] >= y_min-20)
        Niv_1 = Niveaux[PrixMin & PrixMax]

            
        
        #Définition dictionnaire pour libellés de l'abscisses (remplacer numpas100ticks par l'heure) :
        dico={}
        for i in range(0,df.shape[0]-1):
            num=int(df.iloc[i].NumPas100Ticks)
            ts=df.iloc[i].min_ts.strftime('%H:%M:%S')
            dico.update({num:ts})
        
    
        #--------------------
        # Création du Graphe :
        #--------------------
    
        increase=df.close100t>df.open100t
        decrease=df.open100t>df.close100t
        w=0.8 # 2.5 min en ms
        WIDGETS = "pan, wheel_zoom, box_zoom, reset, save"
    
        #p = figure(tools=WIDGETS, plot_width=500, plot_height=400, title = "DAX Bougies",sizing_mode="stretch_both")
        Titre="DAX - "+DateStr
        p = figure(tools=WIDGETS, plot_width=1500, plot_height=600, title = Titre,sizing_mode="stretch_both")
        p.y_range = Range1d(y_min-50, y_max+50)
        p.xaxis.major_label_orientation = pi/4
        #p.xaxis.axis_label=date0+' - num_evt:'+str(num_evt)+' - '+heure0
        p.grid.grid_line_alpha=0.9
        p.xaxis.axis_line_alpha=0.9
        p.xaxis.major_tick_in=10
        p.xaxis.major_tick_out=10
        #p.xaxis.minor_tick_line_cap=100
        Vert="#65e665"
        Vert_Old="#3FBF3F"
    
        # Marquarge des heures KO pour trading (fichier horaire):
        TsKO=(HorairesJour.NiveauRisque == "KO")
        #p.rect((HorairesJour.first_NumPas100Ticks[TsKO]+HorairesJour.last_NumPas100Ticks[TsKO])/2, y_min-2*y_pas, 
        #       (HorairesJour.last_NumPas100Ticks[TsKO]-HorairesJour.first_NumPas100Ticks[TsKO]+1), y_max,
        #        fill_color="#D4EBF5", color="white")        
        p.vbar((HorairesJour.first_NumPas100Ticks[TsKO]+HorairesJour.last_NumPas100Ticks[TsKO])/2, 
               (HorairesJour.last_NumPas100Ticks[TsKO]-HorairesJour.first_NumPas100Ticks[TsKO]+1), 
                y_min, y_max, fill_color="#D4EBF5", 
               line_color="white")

    
        # Bougies 100 ticks:
        p.segment(df.NumPas100Ticks, df.high100t, df.NumPas100Ticks, df.low100t, color="black")
        p.vbar(df.NumPas100Ticks[increase], w, df.open100t[increase], df.close100t[increase], fill_color=Vert, 
               line_color="black")
        p.vbar(df.NumPas100Ticks[decrease], w, df.open100t[decrease], df.close100t[decrease], fill_color="#F2583E", 
               line_color="black")
    
        # Lignes verticales toutes les 30 minutes :
        p.segment(by30min.first_NumPas100Ticks,y_max,by30min.first_NumPas100Ticks,y_min, color="grey")
        p.text(x=by30min.first_NumPas100Ticks,y=y_min-2*y_pas,text=by30min.min_ts.apply(lambda x: pd.Timestamp(x).strftime("%H:%M")),
               text_font_size="8pt",color="black",text_align="left", 
               text_baseline="middle",  angle=pi/2,text_font_style='bold')

        # 2e axe pour afficher le débit :
        debit_max=by100ticks['debit'].max()
        print("debit max:",debit_max)
        p.extra_y_ranges = {"debit": Range1d(start=0, end=3*debit_max)}                
        p.add_layout(LinearAxis(y_range_name="debit"), 'right')     
        debit_petit = df.debit < 3.5
        debit_haut  = df.debit > 5
        p.rect(df.NumPas100Ticks, 0, w, df.debit,
                fill_color="#fcff5e", color="orange", y_range_name="debit")        
        p.rect(df.NumPas100Ticks[debit_petit], 0, w, df.debit[debit_petit],
                fill_color="#D5E1DD", color="green", y_range_name="debit")        
        p.rect(df.NumPas100Ticks[debit_haut], 0, w, df.debit[debit_haut],
                fill_color="#c897e6", color="purple", y_range_name="debit")        
        
        
        # Ajout des Niveaux :

        print(Niv_1)
        
        for i in Niv_1.index:
            Nivo  = Niv_1.loc[i,'Prix']
            texte = Niv_1.loc[i,'Nom'] + ' - ' + '%.1f' %(Nivo)
           
            NomDuNiveau = Niv_1.loc[i,'Nom']
            epaisseur=1
            if NomDuNiveau[-1] == "M":
                epaisseur=5
            if NomDuNiveau[-1] == "S":
                epaisseur=3
            if NomDuNiveau[-1] == "J":
                epaisseur=2
            
            if NomDuNiveau[0] == "m":
                TypeNiveau = NomDuNiveau[1]
            elif NomDuNiveau[0] in ["B", "H", "S", "R", "P"]:
                TypeNiveau = NomDuNiveau[0]
            else:
                 TypeNiveau = NomDuNiveau[-3:]
            
            
            if TypeNiveau == "R":
                couleur = "red"
            elif TypeNiveau == "S":
                couleur = "green"
            elif TypeNiveau == "P":
                couleur = "black"
            elif TypeNiveau in ["H", "B"]:
                couleur = "blue"
            else:
                couleur="black"
            
            #print("NomDuNiveau:", NomDuNiveau, "- Epaisseur:", epaisseur, "- TypeNiveau:", TypeNiveau, "- couleur:", couleur)
                
            p.line([num_deb-2*x_pas,num_fin],[Nivo,Nivo], color=couleur,line_width=epaisseur)
            
            if i % 2 == 0:
                p.text(x=num_deb-2*x_pas,y=Nivo+2,text=[texte],text_font_size="8pt",color="black",text_align="left", 
                       text_baseline="middle", angle=0,text_font_style='bold')
            else:
                p.text(x=num_fin,y=Nivo+2,text=[texte],text_font_size="8pt",color="black",text_align="left", 
                       text_baseline="middle", angle=0,text_font_style='bold')
    
        '''Nivo=Niv_1[0][1]
        p.line([num_deb,num_fin],[Nivo,Nivo], color="red")
        if Type0=='Short':
            p.line([num_deb,num_fin],[Nivo-1,Nivo-1], color="grey")
            p.line([num_deb,num_fin],[Nivo-4,Nivo-4], color="grey")
            p.inverted_triangle(x=[num_evt], y=[Nivo0+y_pas], size=20, color="#DE2D26")
            p.text(x=num_evt,y=Nivo0+2*y_pas,text=[str(Type0)+'\n'+'%.1f' %(Nivo)],text_font_size="12pt",color="#DE2D26",
                   text_align="center", text_baseline="middle", angle=0,text_font_style='bold')
        else:
            p.line([num_deb,num_fin],[Nivo+1,Nivo+1], color="grey")
            p.line([num_deb,num_fin],[Nivo+4,Nivo+4], color="grey")
            p.triangle(x=[num_evt], y=[Nivo0-y_pas], size=20, color="#DE2D26")
            p.text(x=num_evt,y=Nivo0-2*y_pas,text=[str(Type0)+'\n'+'%.1f' %(Nivo)],text_font_size="12pt",color="#DE2D26",
                   text_align="center", text_baseline="middle", angle=0,text_font_style='bold')
        '''
        
        # Ajout des autres positions prises dans la fenetre du graphe :
        def add_position(evt,nivo,sens,ok):
            F_evt=(MesPositions['NumPas100Ticks'] == evt)
            texte_legende=" "+MesPositions[F_evt]['TsExec'].values[0][-8:] +' - '+str(MesPositions[F_evt]['Niveau'].values[0])+' - '+ str(MesPositions[F_evt]['Comment'].values[0])
            if ok=='KO':
                couleur_triangle="red"
            elif ok=='OK':
                couleur_triangle="green"
            elif ok=='Débit':
                couleur_triangle="orange"
            elif ok=='Range':
                couleur_triangle="yellow"
            elif ok=='Heure':
                couleur_triangle="purple"
            else:
                couleur_triangle="black"
                 
            if sens=='SELL':
                p.line([evt-2*x_pas,evt+2*x_pas],[nivo,nivo], color="grey", line_dash="dashed")
                p.line([evt-2*x_pas,evt+2*x_pas],[nivo-3,nivo-3], color="green", line_dash="dashed")
                p.line([evt-2*x_pas,evt+2*x_pas],[nivo+15,nivo+15], color="red", line_dash="dashed")
                p.line([evt,evt],[nivo+2*y_pas,nivo], color="grey", line_dash="dashed")
                p.vbar(evt, 4*w, nivo+2*y_pas, nivo+9*y_pas, fill_color="white", line_color="white")
                p.inverted_triangle(x=[evt], y=[nivo+2*y_pas], size=15, color=couleur_triangle,line_color='black')
                p.text(x=evt,y=nivo+2*y_pas+3,text=[texte_legende],text_font_size="10pt",color="black",
                       text_align="left", text_baseline="middle", angle=pi/2,text_font_style='normal')
                    
            else:
                p.line([evt-2*x_pas,evt+2*x_pas],[nivo,nivo], color="grey", line_dash="dashed")
                p.line([evt-2*x_pas,evt+2*x_pas],[nivo+3,nivo+3], color="green", line_dash="dashed")
                p.line([evt-2*x_pas,evt+2*x_pas],[nivo-15,nivo-15], color="red", line_dash="dashed")
                p.line([evt,evt],[nivo-2*y_pas,nivo], color="grey", line_dash="dashed")
                p.vbar(evt, 4*w, nivo-10*y_pas, nivo-2*y_pas, fill_color="white", line_color="white")
                p.triangle(x=[evt], y=[nivo-2*y_pas], size=15, color=couleur_triangle,line_color='black')
                p.text(x=evt,y=nivo-2*y_pas-3,text=[texte_legende],text_font_size="10pt",color="black",
                       text_align="right", text_baseline="middle", angle=pi/2,text_font_style='normal')
                    
     
        Fdeb=(MesPositions['NumPas100Ticks'] >= num_deb) 
        Ffin=(MesPositions['NumPas100Ticks'] <= num_fin)
        df2=MesPositions[Fdeb&Ffin]
        df2
        offset=y_pas
        for i_evt in df2['NumPas100Ticks']:
            #L=result_full[(result_full['NumPas100Ticks']==i)].index.values[0] #indice
            F3=(MesPositions['NumPas100Ticks'] == i_evt)
            Col_i_evt=MesPositions[F3]['Niveau'].values[0]
            Nivo_i_evt=MesPositions[F3]['Prix'].values[0]
            Type_i_evt=MesPositions[F3]['Sens'].values[0]
            Desc_i_evt=MesPositions[F3]['Comment'].values[0]
            #print('num='+str(i_evt)+' - Niveau='+'%.1f' %(Nivo_i_evt)+' - Type='+str(Type_i_evt)+' - Desc='+str(Desc_i_evt))
            if offset==y_pas:
                    offset=0
            else:
                offset=y_pas
            add_position(i_evt,Nivo_i_evt,Type_i_evt,Desc_i_evt)
        
        
        p.xaxis.major_label_overrides = dico
        #p.xaxis.minor_label_overrides = dico
        #output_file("candlestick.html", title="candlestick.py example")
        #export_png(p, filename="plot"+DateStr+".png")
        
        #if num_evt==2682:
        show(p)
                




    def keyboardInterrupt(self):
        self.nKeybInt += 1
        if self.nKeybInt == 1:
            self.stop()
        else:
            print("Finishing test")
            '''
            print("==============================================================")
            print("    Historique des ordres ")
            print("==============================================================")
            print(self.Histo_Mes_Ordres)
            print("==============================================================")
            '''
            self.done = True

    def stop(self):
        print("Executing cancels")
        self.orderOperations_cancel()
        #self.accountOperations_cancel()
        #self.tickDataOperations_cancel()
        #self.marketDepthOperations_cancel()
        #self.realTimeBarsOperations_cancel()
        #self.historicalDataOperations_cancel()
        #self.optionsOperations_cancel()
        #self.marketScanners_cancel()
        #self.fundamentalsOperations_cancel()
        #self.bulletinsOperations_cancel()
        #self.newsOperations_cancel()
        #self.pnlOperations_cancel()
        #self.histogramOperations_cancel()
        #self.continuousFuturesOperations_cancel()
        self.tickByTickOperations_cancel()
        print("Executing cancels ... finished")
        print("==============================================================")
        print("    Historique des ordres ")
        print("==============================================================")
        print(self.Histo_Mes_Ordres)
        print("==============================================================")

    def increment_id(self):
        """ Increments the request id"""

        self.reqID = self.reqID + 1

    def nextOrderId(self):
        oid = self.nextValidOrderId
        self.nextValidOrderId += 1
        return oid

    @iswrapper
    # ! [error]
    def error(self, reqId: TickerId, errorCode: int, errorString: str):
        super().error(reqId, errorCode, errorString)
        print("Error. Id:", reqId, "Code:", errorCode, "Msg:", errorString)

    # ! [error] self.reqId2nErr[reqId] += 1


    @iswrapper
    def winError(self, text: str, lastError: int):
        super().winError(text, lastError)

    def create_contract(self, NomContrat, EcheanceContrat):
        """ Creates an IB contract."""

        contract = Contract()

        #referentiel des contrats : https://misc.interactivebrokers.com/cstools/contract_info/v3.10/index.php        
        
        if NomContrat=="DAX-mini":
            contract.symbol = "DAX"
            contract.secType = "FUT"
            contract.exchange = "EUREX"
            contract.currency = "EUR"
            contract.lastTradeDateOrContractMonth = EcheanceContrat  #202106
            contract.multiplier = "5"

        elif NomContrat=="DOW-mini":
            contract.symbol = "YM"
            contract.secType = "FUT"
            contract.exchange = "ECBOT"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = EcheanceContrat
            contract.multiplier = "5"

        elif NomContrat=="NASDAQ-mini":
            contract.symbol = "NQ"
            contract.secType = "FUT"
            contract.exchange = "CME"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = EcheanceContrat
            contract.multiplier = "20"
        else:
            print("Contrat non implémenté:", NomContrat)

        return contract

    def create_order(self, action, limit, quantity):
        """ Creates an IB order."""

        order = Order()
        order.action = action
        order.totalQuantity = quantity
        #◘r.account = self.account
        order.orderType = 'LMT'
        order.lmtPrice= limit
        self.increment_id()
        order.orderId = self.reqID

        return order

    def supprimer_Ordres_Parents(self,comment):
        
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        print(timeStr, " - supprimer_Ordres_Parents" )

        F=(self.Histo_Croisements_Niveaux['OrderID']>0)
        for ID in self.Histo_Croisements_Niveaux.loc[F,'OrderID']:
            print("   Demande Cancel ordre", ID)
            self.Histo_Mes_Ordres.loc[ID, "Comment"] = comment
            self.cancelOrder(ID)


    @iswrapper
    # ! [openorder]
    def openOrder(self, orderId: OrderId, contract: Contract, order: Order,
                  orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        if self.demande_open_orders:
            print('------------------------------------------------------------------------------------------------')
            print('-                               LISTE DES ORDRES ENCORE OUVERTS                                -')
            print('------------------------------------------------------------------------------------------------')
            self.demande_open_orders= False
        print(timeStr, " - OpenOrder -   Id:", orderId, "- ParentID:", order.parentId,
              "- Status:", orderState.status, 
              "- Action:", order.action, "- OrderType:", order.orderType,
              "- TotalQty:", order.totalQuantity, 
              "- LmtPrice:", order.lmtPrice, "- AuxPrice:", order.auxPrice, 
              "- PermId: ", order.permId, "- ClientId:", order.clientId,  
              "- Account:", order.account, "- Symbol:", contract.symbol, "- SecType:", contract.secType)

        order.contract = contract
        self.permId2ord[order.permId] = order
    # ! [openorder]

    @iswrapper
    # ! [openorderend]
    def openOrderEnd(self):
        super().openOrderEnd()
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        #print(timeStr, "OpenOrderEnd")
        #print('------------------------------------------------------------------------------------------------')

        logging.debug("Received %d openOrders", len(self.permId2ord))
    # ! [openorderend]

    @iswrapper
    # ! [orderstatus]
    def orderStatus(self, orderId: OrderId, status: str, filled: float,
                    remaining: float, avgFillPrice: float, permId: int,
                    parentId: int, lastFillPrice: float, clientId: int,
                    whyHeld: str, mktCapPrice: float):
        super().orderStatus(orderId, status, filled, remaining,
                            avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        print(timeStr, " - OrderStatus - Id:", orderId, "- ParentId:", parentId,
              "- Status:", status, "- Filled:", filled,
              "- Remaining:", remaining, "- AvgFillPrice:", avgFillPrice,
              "- PermId:", permId,  "- LastFillPrice:",
              lastFillPrice, "- ClientId:", clientId, "- WhyHeld:",
              whyHeld, "- MktCapPrice:", mktCapPrice)
        
        if status == "Cancelled" or status == "ApiCancelled":
            F=(self.Histo_Croisements_Niveaux['OrderID']==orderId)
            if self.Histo_Croisements_Niveaux.loc[F].shape[0] == 1:
                self.Histo_Croisements_Niveaux.loc[F,'OrderID']=0
            
        self.Histo_Mes_Ordres.loc[orderId,['Etat','NbTrt','NbRest']] = [status, filled, remaining]

        # Mise à jour historisation quand la position est soldée (TP ou SL) :
        if status == "Filled" and remaining == 0 and parentId > 0 :
            F=(self.Histo_Croisements_Niveaux['OrderID']==parentId)
            if self.Histo_Croisements_Niveaux.loc[F].shape[0] == 1:
                self.Histo_Croisements_Niveaux.loc[F,'OrderID']=0
                
                # Mise à jour Bilan position
                
                if self.Histo_Mes_Ordres.loc[parentId,'Sens'] == "BUY" :
                    NbSortie = self.Histo_Mes_Ordres.loc[parentId,'Nb']
                    NbEntree = NbSortie * (-1)
                else:
                    NbEntree = self.Histo_Mes_Ordres.loc[parentId,'Nb']
                    NbSortie = NbEntree * (-1)
                    
                PrixEntree  = self.Histo_Mes_Ordres.loc[parentId,'PrixExec']
                PrixSortie  = avgFillPrice
                self.Histo_Mes_Ordres.loc[orderId,'Bilan'] = (NbSortie * PrixSortie) + (NbEntree * PrixEntree)
                
        
        print("==========================================================================")        
        print(timeStr, "- orderStatus - Historique des Ordres positionnés ")        
        print("==========================================================================")        
        F=(self.Histo_Mes_Ordres['Etat'] != 'Cancelled')
        print(self.Histo_Mes_Ordres.loc[F])        
        print("==========================================================================")        
                
            
    # ! [orderstatus]


    def print_MesOrdres(self):
        
        taille = self.Histo_Mes_Ordres.shape[0]
        Titre = "ID Niveau  Bracket  Sens  Prix  Nb      Etat   NbTrt NbRest PrixExec Bilan"

    @printWhenExecuting
    def accountOperations_req(self):
        # Requesting managed accounts
        # ! [reqmanagedaccts]
        self.reqManagedAccts()
        # ! [reqmanagedaccts]

        # Requesting family codes
        # ! [reqfamilycodes]
        self.reqFamilyCodes()
        # ! [reqfamilycodes]

        # Requesting accounts' summary
        # ! [reqaaccountsummary]
        self.reqAccountSummary(9001, "All", AccountSummaryTags.AllTags)
        # ! [reqaaccountsummary]

        # ! [reqaaccountsummaryledger]
        self.reqAccountSummary(9002, "All", "$LEDGER")
        # ! [reqaaccountsummaryledger]

        # ! [reqaaccountsummaryledgercurrency]
        self.reqAccountSummary(9003, "All", "$LEDGER:EUR")
        # ! [reqaaccountsummaryledgercurrency]

        # ! [reqaaccountsummaryledgerall]
        self.reqAccountSummary(9004, "All", "$LEDGER:ALL")
        # ! [reqaaccountsummaryledgerall]

        # Subscribing to an account's information. Only one at a time!
        # ! [reqaaccountupdates]
        self.reqAccountUpdates(True, self.account)
        # ! [reqaaccountupdates]

        # ! [reqaaccountupdatesmulti]
        self.reqAccountUpdatesMulti(9005, self.account, "", True)
        # ! [reqaaccountupdatesmulti]

        # Requesting all accounts' positions.
        # ! [reqpositions]
        self.reqPositions()
        # ! [reqpositions]

        # ! [reqpositionsmulti]
        self.reqPositionsMulti(9006, self.account, "")
        # ! [reqpositionsmulti]

    @printWhenExecuting
    def accountOperations_cancel(self):
        # ! [cancelaaccountsummary]
        self.cancelAccountSummary(9001)
        self.cancelAccountSummary(9002)
        self.cancelAccountSummary(9003)
        self.cancelAccountSummary(9004)
        # ! [cancelaaccountsummary]

        # ! [cancelaaccountupdates]
        self.reqAccountUpdates(False, self.account)
        # ! [cancelaaccountupdates]

        # ! [cancelaaccountupdatesmulti]
        self.cancelAccountUpdatesMulti(9005)
        # ! [cancelaaccountupdatesmulti]

        # ! [cancelpositions]
        self.cancelPositions()
        # ! [cancelpositions]

        # ! [cancelpositionsmulti]
        self.cancelPositionsMulti(9006)
        # ! [cancelpositionsmulti]

    def pnlOperations_req(self):
        # ! [reqpnl]
        self.reqPnL(17001, "DU111519", "")
        # ! [reqpnl]

        # ! [reqpnlsingle]
        self.reqPnLSingle(17002, "DU111519", "", 8314);
        # ! [reqpnlsingle]

    def pnlOperations_cancel(self):
        # ! [cancelpnl]
        self.cancelPnL(17001)
        # ! [cancelpnl]

        # ! [cancelpnlsingle]
        self.cancelPnLSingle(17002);
        # ! [cancelpnlsingle]

    def histogramOperations_req(self):
        # ! [reqhistogramdata]
        self.reqHistogramData(4002, ContractSamples.USStockAtSmart(), False, "3 days");
        # ! [reqhistogramdata]

    def histogramOperations_cancel(self):
        # ! [cancelhistogramdata]
        self.cancelHistogramData(4002);
        # ! [cancelhistogramdata]

    def continuousFuturesOperations_req(self):
        # ! [reqcontractdetailscontfut]
        self.reqContractDetails(18001, ContractSamples.ContFut())
        # ! [reqcontractdetailscontfut]

        # ! [reqhistoricaldatacontfut]
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        self.reqHistoricalData(18002, ContractSamples.ContFut(), timeStr, "1 Y", "1 month", "TRADES", 0, 1, False, []);
        # ! [reqhistoricaldatacontfut]

    def continuousFuturesOperations_cancel(self):
        # ! [cancelhistoricaldatacontfut]
        self.cancelHistoricalData(18002);
        # ! [cancelhistoricaldatacontfut]

    @iswrapper
    # ! [managedaccounts]
    def managedAccounts(self, accountsList: str):
        super().managedAccounts(accountsList)
        print("Account list:", accountsList)
        # ! [managedaccounts]

        self.account = accountsList.split(",")[0]

    @iswrapper
    # ! [accountsummary]
    def accountSummary(self, reqId: int, account: str, tag: str, value: str,
                       currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        print("AccountSummary. ReqId:", reqId, "Account:", account,
              "Tag: ", tag, "Value:", value, "Currency:", currency)
    # ! [accountsummary]

    @iswrapper
    # ! [accountsummaryend]
    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        print("AccountSummaryEnd. ReqId:", reqId)
    # ! [accountsummaryend]

    @iswrapper
    # ! [updateaccountvalue]
    def updateAccountValue(self, key: str, val: str, currency: str,
                           accountName: str):
        super().updateAccountValue(key, val, currency, accountName)
        print("UpdateAccountValue. Key:", key, "Value:", val,
              "Currency:", currency, "AccountName:", accountName)
    # ! [updateaccountvalue]

    @iswrapper
    # ! [updateportfolio]
    def updatePortfolio(self, contract: Contract, position: float,
                        marketPrice: float, marketValue: float,
                        averageCost: float, unrealizedPNL: float,
                        realizedPNL: float, accountName: str):
        super().updatePortfolio(contract, position, marketPrice, marketValue,
                                averageCost, unrealizedPNL, realizedPNL, accountName)
        print("UpdatePortfolio.", "Symbol:", contract.symbol, "SecType:", contract.secType, "Exchange:",
              contract.exchange, "Position:", position, "MarketPrice:", marketPrice,
              "MarketValue:", marketValue, "AverageCost:", averageCost,
              "UnrealizedPNL:", unrealizedPNL, "RealizedPNL:", realizedPNL,
              "AccountName:", accountName)
    # ! [updateportfolio]

    @iswrapper
    # ! [updateaccounttime]
    def updateAccountTime(self, timeStamp: str):
        super().updateAccountTime(timeStamp)
        print("UpdateAccountTime. Time:", timeStamp)
    # ! [updateaccounttime]

    @iswrapper
    # ! [accountdownloadend]
    def accountDownloadEnd(self, accountName: str):
        super().accountDownloadEnd(accountName)
        print("AccountDownloadEnd. Account:", accountName)
    # ! [accountdownloadend]

    @iswrapper
    # ! [position]
    def position(self, account: str, contract: Contract, position: float,
                 avgCost: float):
        super().position(account, contract, position, avgCost)
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        print(timeStr, "Position.", "Account:", account, "Symbol:", contract.symbol, "SecType:",
              contract.secType, "Currency:", contract.currency,
              "Position:", position, "Avg cost:", avgCost)
    # ! [position]

    @iswrapper
    # ! [positionend]
    def positionEnd(self):
        super().positionEnd()
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        print(timeStr, "PositionEnd")
    # ! [positionend]

    @iswrapper
    # ! [positionmulti]
    def positionMulti(self, reqId: int, account: str, modelCode: str,
                      contract: Contract, pos: float, avgCost: float):
        super().positionMulti(reqId, account, modelCode, contract, pos, avgCost)
        print("PositionMulti. RequestId:", reqId, "Account:", account,
              "ModelCode:", modelCode, "Symbol:", contract.symbol, "SecType:",
              contract.secType, "Currency:", contract.currency, ",Position:",
              pos, "AvgCost:", avgCost)
    # ! [positionmulti]

    @iswrapper
    # ! [positionmultiend]
    def positionMultiEnd(self, reqId: int):
        super().positionMultiEnd(reqId)
        print("PositionMultiEnd. RequestId:", reqId)
    # ! [positionmultiend]

    @iswrapper
    # ! [accountupdatemulti]
    def accountUpdateMulti(self, reqId: int, account: str, modelCode: str,
                           key: str, value: str, currency: str):
        super().accountUpdateMulti(reqId, account, modelCode, key, value,
                                   currency)
        print("AccountUpdateMulti. RequestId:", reqId, "Account:", account,
              "ModelCode:", modelCode, "Key:", key, "Value:", value,
              "Currency:", currency)
    # ! [accountupdatemulti]

    @iswrapper
    # ! [accountupdatemultiend]
    def accountUpdateMultiEnd(self, reqId: int):
        super().accountUpdateMultiEnd(reqId)
        print("AccountUpdateMultiEnd. RequestId:", reqId)
    # ! [accountupdatemultiend]

    @iswrapper
    # ! [familyCodes]
    def familyCodes(self, familyCodes: ListOfFamilyCode):
        super().familyCodes(familyCodes)
        print("Family Codes:")
        for familyCode in familyCodes:
            print("FamilyCode.", familyCode)
    # ! [familyCodes]

    @iswrapper
    # ! [pnl]
    def pnl(self, reqId: int, dailyPnL: float,
            unrealizedPnL: float, realizedPnL: float):
        super().pnl(reqId, dailyPnL, unrealizedPnL, realizedPnL)
        print("Daily PnL. ReqId:", reqId, "DailyPnL:", dailyPnL,
              "UnrealizedPnL:", unrealizedPnL, "RealizedPnL:", realizedPnL)
    # ! [pnl]

    @iswrapper
    # ! [pnlsingle]
    def pnlSingle(self, reqId: int, pos: int, dailyPnL: float,
                  unrealizedPnL: float, realizedPnL: float, value: float):
        super().pnlSingle(reqId, pos, dailyPnL, unrealizedPnL, realizedPnL, value)
        print("Daily PnL Single. ReqId:", reqId, "Position:", pos,
              "DailyPnL:", dailyPnL, "UnrealizedPnL:", unrealizedPnL,
              "RealizedPnL:", realizedPnL, "Value:", value)
    # ! [pnlsingle]

    def marketDataTypeOperations(self):
        # ! [reqmarketdatatype]
        # Switch to live (1) frozen (2) delayed (3) delayed frozen (4).
        self.reqMarketDataType(MarketDataTypeEnum.DELAYED)
        # ! [reqmarketdatatype]

    @iswrapper
    # ! [marketdatatype]
    def marketDataType(self, reqId: TickerId, marketDataType: int):
        super().marketDataType(reqId, marketDataType)
        print("MarketDataType. ReqId:", reqId, "Type:", marketDataType)
    # ! [marketdatatype]

    @printWhenExecuting
    def tickDataOperations_req(self):
        self.reqMarketDataType(MarketDataTypeEnum.DELAYED_FROZEN)
        
        # Requesting real time market data

        # ! [reqmktdata]
        self.reqMktData(1000, ContractSamples.USStockAtSmart(), "", False, False, [])
        self.reqMktData(1001, ContractSamples.StockComboContract(), "", False, False, [])
        # ! [reqmktdata]

        # ! [reqmktdata_snapshot]
        self.reqMktData(1002, ContractSamples.FutureComboContract(), "", True, False, [])
        # ! [reqmktdata_snapshot]

        # ! [regulatorysnapshot]
        # Each regulatory snapshot request incurs a 0.01 USD fee
        self.reqMktData(1003, ContractSamples.USStock(), "", False, True, [])
        # ! [regulatorysnapshot]

        # ! [reqmktdata_genticks]
        # Requesting RTVolume (Time & Sales), shortable and Fundamental Ratios generic ticks
        self.reqMktData(1004, ContractSamples.USStockAtSmart(), "233,236,258", False, False, [])
        # ! [reqmktdata_genticks]

        # ! [reqmktdata_contractnews]
        # Without the API news subscription this will generate an "invalid tick type" error
        self.reqMktData(1005, ContractSamples.USStockAtSmart(), "mdoff,292:BRFG", False, False, [])
        self.reqMktData(1006, ContractSamples.USStockAtSmart(), "mdoff,292:BRFG+DJNL", False, False, [])
        self.reqMktData(1007, ContractSamples.USStockAtSmart(), "mdoff,292:BRFUPDN", False, False, [])
        self.reqMktData(1008, ContractSamples.USStockAtSmart(), "mdoff,292:DJ-RT", False, False, [])
        # ! [reqmktdata_contractnews]


        # ! [reqmktdata_broadtapenews]
        self.reqMktData(1009, ContractSamples.BRFGbroadtapeNewsFeed(), "mdoff,292", False, False, [])
        self.reqMktData(1010, ContractSamples.DJNLbroadtapeNewsFeed(), "mdoff,292", False, False, [])
        self.reqMktData(1011, ContractSamples.DJTOPbroadtapeNewsFeed(), "mdoff,292", False, False, [])
        self.reqMktData(1012, ContractSamples.BRFUPDNbroadtapeNewsFeed(), "mdoff,292", False, False, [])
        # ! [reqmktdata_broadtapenews]

        # ! [reqoptiondatagenticks]
        # Requesting data for an option contract will return the greek values
        self.reqMktData(1013, ContractSamples.OptionWithLocalSymbol(), "", False, False, [])
        self.reqMktData(1014, ContractSamples.FuturesOnOptions(), "", False, False, []);
        
        # ! [reqoptiondatagenticks]

        # ! [reqfuturesopeninterest]
        self.reqMktData(1015, ContractSamples.SimpleFuture(), "mdoff,588", False, False, [])
        # ! [reqfuturesopeninterest]

        # ! [reqmktdatapreopenbidask]
        self.reqMktData(1016, ContractSamples.SimpleFuture(), "", False, False, [])
        # ! [reqmktdatapreopenbidask]

        # ! [reqavgoptvolume]
        self.reqMktData(1017, ContractSamples.USStockAtSmart(), "mdoff,105", False, False, [])
        # ! [reqavgoptvolume]
        
        # ! [reqsmartcomponents]
        # Requests description of map of single letter exchange codes to full exchange names
        self.reqSmartComponents(1018, "a6")
        # ! [reqsmartcomponents]
        

    @printWhenExecuting
    def tickDataOperations_cancel(self):
        # Canceling the market data subscription
        # ! [cancelmktdata]
        self.cancelMktData(1000)
        self.cancelMktData(1001)
        # ! [cancelmktdata]

        self.cancelMktData(1004)
        
        self.cancelMktData(1005)
        self.cancelMktData(1006)
        self.cancelMktData(1007)
        self.cancelMktData(1008)
        
        self.cancelMktData(1009)
        self.cancelMktData(1010)
        self.cancelMktData(1011)
        self.cancelMktData(1012)
        
        self.cancelMktData(1013)
        self.cancelMktData(1014)
        
        self.cancelMktData(1015)
        
        self.cancelMktData(1016)
        
        self.cancelMktData(1017)

    @iswrapper
    # ! [tickprice]
    def tickPrice(self, reqId: TickerId, tickType: TickType, price: float,
                  attrib: TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        print("TickPrice. TickerId:", reqId, "tickType:", tickType,
              "Price:", price, "CanAutoExecute:", attrib.canAutoExecute,
              "PastLimit:", attrib.pastLimit, end=' ')
        if tickType == TickTypeEnum.BID or tickType == TickTypeEnum.ASK:
            print("PreOpen:", attrib.preOpen)
        else:
            print()
    # ! [tickprice]

    @iswrapper
    # ! [ticksize]
    def tickSize(self, reqId: TickerId, tickType: TickType, size: int):
        super().tickSize(reqId, tickType, size)
        print("TickSize. TickerId:", reqId, "TickType:", tickType, "Size:", size)
    # ! [ticksize]

    @iswrapper
    # ! [tickgeneric]
    def tickGeneric(self, reqId: TickerId, tickType: TickType, value: float):
        super().tickGeneric(reqId, tickType, value)
        print("TickGeneric. TickerId:", reqId, "TickType:", tickType, "Value:", value)
    # ! [tickgeneric]

    @iswrapper
    # ! [tickstring]
    def tickString(self, reqId: TickerId, tickType: TickType, value: str):
        super().tickString(reqId, tickType, value)
        print("TickString. TickerId:", reqId, "Type:", tickType, "Value:", value)
    # ! [tickstring]

    @iswrapper
    # ! [ticksnapshotend]
    def tickSnapshotEnd(self, reqId: int):
        super().tickSnapshotEnd(reqId)
        print("TickSnapshotEnd. TickerId:", reqId)
    # ! [ticksnapshotend]

    @iswrapper
    # ! [rerouteMktDataReq]
    def rerouteMktDataReq(self, reqId: int, conId: int, exchange: str):
        super().rerouteMktDataReq(reqId, conId, exchange)
        print("Re-route market data request. ReqId:", reqId, "ConId:", conId, "Exchange:", exchange)
    # ! [rerouteMktDataReq]

    @iswrapper
    # ! [marketRule]
    def marketRule(self, marketRuleId: int, priceIncrements: ListOfPriceIncrements):
        super().marketRule(marketRuleId, priceIncrements)
        print("Market Rule ID: ", marketRuleId)
        for priceIncrement in priceIncrements:
            print("Price Increment.", priceIncrement)
    # ! [marketRule]

    @printWhenExecuting
    def tickByTickOperations_req(self):
        
        self.reqTickByTickData_ID = self.reqID
        # Requesting tick-by-tick data (only refresh)
        # ! [reqtickbytick]
        self.reqTickByTickData(self.reqTickByTickData_ID, self.contract, "Last", 0, True)
        #self.reqTickByTickData(19002, ContractSamples.EuropeanStock2(), "AllLast", 0, False)
        #self.reqTickByTickData(19003, ContractSamples.EuropeanStock2(), "BidAsk", 0, True)
        #self.reqTickByTickData(19004, ContractSamples.EurGbpFx(), "MidPoint", 0, False)
        # ! [reqtickbytick]

        # Requesting tick-by-tick data (refresh + historicalticks)
        # ! [reqtickbytickwithhist]
        #self.reqTickByTickData(19005, ContractSamples.EuropeanStock2(), "Last", 10, False)
        #self.reqTickByTickData(19006, ContractSamples.EuropeanStock2(), "AllLast", 10, False)
        #self.reqTickByTickData(19007, ContractSamples.EuropeanStock2(), "BidAsk", 10, False)
        #self.reqTickByTickData(19008, ContractSamples.EurGbpFx(), "MidPoint", 10, True)
        # ! [reqtickbytickwithhist]

    @printWhenExecuting
    def tickByTickOperations_cancel(self):
        # ! [canceltickbytick]
        self.cancelTickByTickData(self.reqTickByTickData_ID)
        #self.cancelTickByTickData(19002)
        #self.cancelTickByTickData(19003)
        #self.cancelTickByTickData(19004)
        # ! [canceltickbytick]

        # ! [canceltickbytickwithhist]
        #self.cancelTickByTickData(19005)
        #self.cancelTickByTickData(19006)
        #self.cancelTickByTickData(19007)
        #self.cancelTickByTickData(19008)
        # ! [canceltickbytickwithhist]
        
    @iswrapper
    # ! [orderbound]
    def orderBound(self, orderId: int, apiClientId: int, apiOrderId: int):
        super().orderBound(orderId, apiClientId, apiOrderId)

        ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")

        print(ts, "- OrderBound.", "Id:", apiOrderId, "orderId:", orderId, "ApiClientId:", apiClientId)
    # ! [orderbound]


    def init_positions(self):
        
            nom_niveau_sup = self.nom_niveau_trading_sup
            nom_niveau_inf = self.nom_niveau_trading_inf
        
            self.ecart_niveau_sup = self.niveau_trading_sup - self.prix_courant
            self.ecart_niveau_inf = self.prix_courant - self.niveau_trading_inf
            
            CreerOrdreSup=False
            CreerOrdreInf=False
            
            # Si un ordre est déjà positionné, nul besoin de tout recalculer:
            if self.Histo_Croisements_Niveaux.loc[nom_niveau_sup,'OrderID'] == 0:
                # Si on a franchi le trigger :
                if self.ecart_niveau_sup <= self.ParamDistanceTriggers:
                    CreerOrdreSup=True
                    print('   - Niveau supérieur : le prix actuel est au-dessus du Trigger du niveau supérieur {:s} - Position COURTE à positionner...'.format(nom_niveau_sup))
                else:
                    print("   - Niveau supérieur : le prix actuel est trop loin du niveau supérieur pour y placer un ordre SHORT.")
                
            # Si un ordre est déjà positionné, nul besoin de tout recalculer:
            if self.Histo_Croisements_Niveaux.loc[nom_niveau_inf,'OrderID'] == 0:
                # Si on a franchi le trigger :
                if self.ecart_niveau_inf <= self.ParamDistanceTriggers:
                    CreerOrdreInf=True
                    print('   - Niveau inférieur : le prix actuel est au-dessous du Trigger du niveau inférieur {:s} - Ordre LONGUE à positionner...'.format(nom_niveau_inf))
                else:
                    print("   - Niveau inférieur : le prix actuel est trop loin du niveau inférieur pour y placer un ordre LONG.")
            
 
            if CreerOrdreSup:
                self.OpenPositionCourte_S1_req(nom_niveau_sup, self.niveau_trading_sup+4, 1)
            
            if CreerOrdreInf:
                self.OpenPositionLongue_S1_req(nom_niveau_inf, self.niveau_trading_inf-4,1)
                

    @iswrapper
    # ! [tickbytickalllast]
    def tickByTickAllLast(self, reqId: int, tickType: int, time: int, price: float,
                          size: int, tickAtrribLast: TickAttribLast, exchange: str,
                          specialConditions: str):
        super().tickByTickAllLast(reqId, tickType, time, price, size, tickAtrribLast,
                                  exchange, specialConditions)

        self.numTick=self.numTick+1
  
        # Affichage des ticks si on est proche des positions ouvertes :
        '''
        if self.numTick>3:
            if self.NiveauRisqueVolumesActuels == "OK" and self.NiveauRisquePeriodeCourante == "OK" :
                
                if    ( ( self.Histo_Croisements_Niveaux.loc[self.nom_niveau_trading_sup,'OrderID'] > 0 and self.prix_courant > (self.niveau_trading_sup-5)) or
                        (self.Histo_Croisements_Niveaux.loc[self.nom_niveau_trading_inf,'OrderID'] > 0 and self.prix_courant < (self.niveau_trading_inf+5))) : 
                    print(datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"),
                         " - tickByTickAllLast - tick ", self.numTick, "- ReqId:", reqId,
                         "- Size:", size, "- Niveau inf:", self.niveau_trading_inf, 
                         "- Price:", price, "- Niveau Sup:", self.niveau_trading_sup)
        
        '''
        ts = datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S")



        # Niveau de risque de la période
        if self.numTick>1:
            self.NiveauRisquePeriodePrec     = self.NiveauRisquePeriodeCourante
            self.NiveauRisquePeriodeCourante = self.HorairesDuJour.get_Risque(time)        
            sortiePeriodeNiveauRisqueKO = self.NiveauRisquePeriodeCourante == "OK" and self.NiveauRisquePeriodePrec == "KO"
            entreePeriodeNiveauRisqueKO = self.NiveauRisquePeriodeCourante == "KO" and self.NiveauRisquePeriodePrec == "OK"
        
            if entreePeriodeNiveauRisqueKO:
                print("-------------------------------------------------------------------------------------------------------------------------------------------")
                print("-------------------------------------------------------------------------------------------------------------------------------------------")
                print(ts, "Entree dans une période non favorable au trading (heure) jusqu'à {:s}. Attente fin période risquée...".format(self.HorairesDuJour.get_next_heure_OK_pour_trading()))
                print("-------------------------------------------------------------------------------------------------------------------------------------------")
                print("-------------------------------------------------------------------------------------------------------------------------------------------")
                self.supprimer_Ordres_Parents("Heure non favorable (news,...)")

            if sortiePeriodeNiveauRisqueKO:
                print("-------------------------------------------------------------------------------------------------------------------------------------------")
                print("-------------------------------------------------------------------------------------------------------------------------------------------")
                print(ts, "Sortie de période de risque élevé (heure)")
                print("-------------------------------------------------------------------------------------------------------------------------------------------")
                print("-------------------------------------------------------------------------------------------------------------------------------------------")
                if self.NiveauRisqueVolumesActuels == "OK":
                    self.init_positions()
            

        # Calcul du débit de ticks tous les Q ticks
        NbTicksPourCalculDebit = self.ParamDureePeriodecalculDebit
        if (self.numTick % NbTicksPourCalculDebit == 0):
            if (self.numTick > 2 * NbTicksPourCalculDebit):
                self.NiveauRisqueVolumesPrec = self.NiveauRisqueVolumesActuels
                self.CalculDebit_TsPrec = self.CalculDebit_TsCour
                self.CalculDebit_TsCour = time
                
                DeltaSec = self.CalculDebit_TsCour - self.CalculDebit_TsPrec
                if DeltaSec == 0:
                    self.NiveauRisqueVolumesActuels = "KO"
                    #print(ts, "Niveau de risque elevé : débit supérieur à la normale (plus de 50 ticks /sec)")
                    DebitTicks=50
                else:
                    DebitTicks = NbTicksPourCalculDebit / DeltaSec
                    
                self.DebitCourant = DebitTicks

                if DebitTicks >= self.ParamDebitTicksMax:
                    self.NiveauRisqueVolumesActuels = "KO"
                    #print(ts, "Niveau de risque elevé : débit supérieur à la normale",
                    #  DebitTicks, "ticks/sec")
                else:
                    self.NiveauRisqueVolumesActuels = "OK"
                
                
                if self.NiveauRisquePeriodeCourante == "OK" and self.NiveauRisqueVolumesActuels == "KO" and self.NiveauRisqueVolumesPrec == "OK":
                    print("-------------------------------------------------------------------------------------------------------------------------------------------")
                    print("-------------------------------------------------------------------------------------------------------------------------------------------")
                    print(ts, "Entree dans une période non favorable au trading (débit: {:0.2F} supérieur au seuil de {:0.2F}). Attente fin période risquée...".format(DebitTicks, self.ParamDebitTicksMax))
                    print("-------------------------------------------------------------------------------------------------------------------------------------------")
                    print("-------------------------------------------------------------------------------------------------------------------------------------------")
                    self.NbPeriodesDebitOK = 0
                    self.supprimer_Ordres_Parents("Débit trop élevé")
                    
                if self.NiveauRisquePeriodeCourante == "OK" and self.NiveauRisqueVolumesActuels == "OK" and self.NiveauRisqueVolumesPrec == "KO":
                    self.NbPeriodesDebitOK = 1

                if (self.NiveauRisquePeriodeCourante == "OK" and self.NiveauRisqueVolumesActuels == "OK" and self.NbPeriodesDebitOK >= 1 
                    and self.NbPeriodesDebitOK <= self.ParamNbPeriodeDebitOK):
                    self.NbPeriodesDebitOK = self.NbPeriodesDebitOK + 1
                    
                    if self.NbPeriodesDebitOK == self.ParamNbPeriodeDebitOK:
                        print("-------------------------------------------------------------------------------------------------------------------------------------------")
                        print("-------------------------------------------------------------------------------------------------------------------------------------------")
                        print(ts, "Sortie de période de risque élevé liée aux volumes, Nb périodes OK en débit=", self.NbPeriodesDebitOK )
                        print("-------------------------------------------------------------------------------------------------------------------------------------------")
                        print("-------------------------------------------------------------------------------------------------------------------------------------------")
                        if self.NiveauRisquePeriodeCourante == "OK":
                            self.init_positions()
            
            
            else:
                self.CalculDebit_TsCour = time

        # Vérification situation de range avant validation niveau avec position ouverte : risque de break out
        if self.numTick > 1 :
            FreqVerifRange = self.FrequenceVerificationSituationRange
            if (self.numTick % FreqVerifRange == 0):
                
                # Y at-il des positions parentes à l'état "Submitted" dont le tickValidité est dépassé ?
                F1=(self.Histo_Mes_Ordres['Etat']=="Submitted")
                F2=(self.Histo_Mes_Ordres['Bracket']=="Parent")
                F3=(self.Histo_Mes_Ordres['TickValidite'] < self.numTick)
    
                if self.Histo_Mes_Ordres.loc[F1&F2&F3].shape[0] > 0:
                    
                    #Boucle sur ces positions, pour les annuler :
                    for i in self.Histo_Mes_Ordres.loc[F1&F2&F3].index :
                        i_Sens      = self.Histo_Mes_Ordres.loc[i, "Sens"]
                        i_NomNiveau = self.Histo_Mes_Ordres.loc[i, "Niveau"]     
                        i_tickCross = self.Histo_Mes_Ordres.loc[i, "TickValidite"] - self.DureeVieOrdreParent_Ticks    
                        self.Histo_Mes_Ordres.loc[i, "Comment"] = "Range probable juste avant niveau position"
                        print(ts, "NumTick:", self.numTick," - Trigger croisé depuis trop longtemps. Situation probable de range juste avant ouverture position.", end='')
                        print(" Le trigger de {:s} associé au niveau {:s} a été croisé au tick numéro {:0F} (max autorisé={:0F})".format(i_Sens,  i_NomNiveau, 
                                                                                                                           i_tickCross, 
                                                                                                                           self.DureeVieOrdreParent_Ticks))
                        print("Demande Cancel ordre", i)
                        self.cancelOrder(i)

 
        # Initialisation 
        if self.numTick == 1:
            self.prix_courant = price
        self.prix_prec    = self.prix_courant
        self.prix_courant = price
            
        # Récupération des niveaux à trader
        if self.numTick==1:
            
            ts = datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S")
            print('==================================================================================')
            print('   Package Robot IB.py - tickByTickAllLast - Initialisation NumTick=1')
            print('                          ',ts)
            print('   Dernier prix :',price)
            print('==================================================================================')


            # Récupération des horaires OK/KO pour trader aujourd'hui :
            self.HorairesDuJour=Horaires.Horaires()
            self.HorairesDuJour.Init_Horaires()
            self.NiveauRisquePeriodeCourante = self.HorairesDuJour.get_Risque(time) 


            # Récupération des niveaux à prendre en compte dans stratégie S1 :
            self.Histo_Croisements_Niveaux = self.NiveauDuJour.get_df_histo_niveaux()
            self.NiveauxATrader = self.NiveauDuJour.get_Niveaux_adjacents_a_trader(self.prix_courant, self.ParamDistanceTriggers)
            self.nom_niveau_trading_sup = self.NiveauxATrader.loc['Sup+','Nivo']
            self.nom_niveau_trading_inf = self.NiveauxATrader.loc['Inf-','Nivo']
            self.niveau_trading_sup = self.NiveauxATrader.loc['Sup+','Prix']
            self.niveau_trading_inf = self.NiveauxATrader.loc['Inf-','Prix']
            self.trigger_sup = self.NiveauxATrader.loc['Sup+','Trigger']
            self.trigger_inf = self.NiveauxATrader.loc['Inf-','Trigger']

            self.NivAdj = self.NiveauDuJour.get_Niveaux_adjacents(self.prix_courant)
            self.nom_niveau_sup = self.NivAdj.loc['Sup+','Nivo']
            self.nom_niveau_inf = self.NivAdj.loc['Inf-','Nivo']
            self.niveau_sup     = self.NivAdj.loc['Sup+','Prix']
            self.niveau_inf     = self.NivAdj.loc['Inf-','Prix']

            self.Histo_Croisements_Triggers_H = self.NiveauDuJour.get_Niveaux_H()
            self.Histo_Croisements_Triggers_L = self.NiveauDuJour.get_Niveaux_L()
            
            
            print('----------------------------------------')
            print('---       NIVEAUX A TRADER           ---')
            print('----------------------------------------')
            print(self.NiveauxATrader)
            print('Dernier prix ',price)
            print('----------------------------------------')
             
            if self.NiveauRisquePeriodeCourante == "OK" and self.NiveauRisqueVolumesActuels == "OK":
                print(ts,'Initialisation positions...')
                self.init_positions()
            elif self.NiveauRisquePeriodeCourante == "KO" :
                print(ts, "Période actuelle non favorable au trading (heure) jusqu'à {:s}. Attente fin période risquée...".format(self.HorairesDuJour.get_next_heure_OK_pour_trading()))
            elif self.NiveauRisqueVolumesActuels == "KO":
                print(ts, "Période actuelle non favorable au trading (débit). Attente fin période risquée...")
                

        # Test croissement triggers vers niveaux pour historisation :           
        if (self.trigger_sup > 0 and  
            (self.prix_courant >= self.trigger_sup and self.prix_prec < self.trigger_sup)):
                    
            self.Histo_Croisements_Triggers_H.loc[self.nom_niveau_trading_sup, 'TickDernierCross'] = self.numTick
            F=(self.Histo_Croisements_Triggers_H['TickDernierCross']>0)
            print("================================================================================================================================")
            print(ts, " - tickByTickAllLast - self.Histo_Croisements_Triggers_H", self.nom_niveau_trading_sup," - NumTick:", self.numTick)
            print("================================================================================================================================")

            print(self.Histo_Croisements_Triggers_H.loc[F])

        if (self.trigger_inf > 0 and  
            (self.prix_courant <= self.trigger_inf and self.prix_prec > self.trigger_inf)):

            self.Histo_Croisements_Triggers_L.loc[self.nom_niveau_trading_inf, 'TickDernierCross'] = self.numTick
            F=(self.Histo_Croisements_Triggers_L['TickDernierCross']>0)
            print("================================================================================================================================")
            print(ts, " - tickByTickAllLast - self.Histo_Croisements_Triggers_L", self.nom_niveau_trading_inf," - NumTick:", self.numTick)
            print("================================================================================================================================")

            print(self.Histo_Croisements_Triggers_L.loc[F])

                
        # Test croisement des niveaux triggers pour prise de position :
        if self.NiveauRisquePeriodeCourante == "OK" and self.NiveauRisqueVolumesActuels == "OK":
            
            if self.Histo_Croisements_Niveaux.loc[self.nom_niveau_trading_sup,'OrderID'] == 0:
                if (self.trigger_sup > 0 and  
                    (self.prix_courant >= self.trigger_sup and self.prix_prec < self.trigger_sup)):
        
                    ts = datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S")
        
                    nomNiveau=self.nom_niveau_trading_sup
                    print(ts, "NumTick:", self.numTick, " - tickByTickAllLast - croisement niveau Trigger supérieur (pour niveau ",
                          nomNiveau, "), prix actuel : ", price)
                    if self.Histo_Croisements_Niveaux.loc[nomNiveau,'OrderID'] > 0:
                        print('   Ordre déjà positionné sur ce niveau ', nomNiveau)
                    else:   
                        print('   Nous allons ouvrir une position courte sur le niveau ', nomNiveau)
                        self.OpenPositionCourte_S1_req(nomNiveau, self.niveau_trading_sup+4, 1)
                
            if self.Histo_Croisements_Niveaux.loc[self.nom_niveau_trading_inf,'OrderID'] == 0:
                if (self.trigger_inf > 0 and  
                    (self.prix_courant <= self.trigger_inf and self.prix_prec > self.trigger_inf)):
                    ts = datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S")
        
                    nomNiveau=self.nom_niveau_trading_inf
                    print(ts, "NumTick:", self.numTick, " - tickByTickAllLast - croisement niveau Trigger inférieur (pour niveau ",
                          nomNiveau, "), prix actuel : ", price)
                    if self.Histo_Croisements_Niveaux.loc[nomNiveau,'OrderID'] > 0:
                        print('   Ordre déjà positionné sur ce niveau ', nomNiveau)
                    else:   
                        print('   Nous allons ouvrir une position longue sur le niveau ', nomNiveau)
                        self.OpenPositionLongue_S1_req(nomNiveau, self.niveau_trading_inf-4, 1)
               


            
        # Croisement des niveaux à trader inf et sup : Recalcul des niveaux à trader si 
        if self.prix_courant > self.niveau_trading_sup or self.prix_courant < self.niveau_trading_inf :

            ts = datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S")
            #print(ts, " - tickByTickAllLast - croisement niveau, prix actuel : ", price)

            #Historisation de ce croisement 
            if self.prix_courant > self.niveau_trading_sup:
                nomNiveau  = self.nom_niveau_trading_sup
                prix = self.niveau_trading_sup
                '''
                print("================================================================================================================================")
                print(ts, " - tickByTickAllLast - croisement à la hausse niveau {:s} ({:0.2F}), prix actuel : {:0.2F}".format(nomNiveau, prix, price))
                print("================================================================================================================================")
                '''
            else:
                nomNiveau  = self.nom_niveau_trading_inf
                prix = self.niveau_trading_inf
                '''
                print("================================================================================================================================")
                print(ts, " - tickByTickAllLast - croisement à la baisse niveau {:s} ({:0.2F}), prix actuel : {:0.2F}".format(nomNiveau, prix, price))
                print("================================================================================================================================")
                '''
           
            ts = datetime.datetime.today()
            
            #print('   Historisation du croisement du niveau ', nomNiveau, 'NumTick=', self.numTick)
            HeureStr = datetime.datetime.fromtimestamp(time).strftime("%H:%M:%S")
            Order_ID0   = self.Histo_Croisements_Niveaux.loc[nomNiveau,'OrderID']
            numTickAntePrec = self.Histo_Croisements_Niveaux.loc[nomNiveau,'numTickPrec']
            tsAntePrec = self.Histo_Croisements_Niveaux.loc[nomNiveau,'TSPrec']
            self.Histo_Croisements_Niveaux.loc[nomNiveau]=[prix, tsAntePrec, numTickAntePrec, HeureStr, self.numTick, Order_ID0]
            F1=(self.Histo_Croisements_Niveaux['numTickPrec']>0)
            F2=(self.Histo_Croisements_Niveaux['OrderID']>0)
            '''print(" === Liste des niveaux déjà croisés === ")
            print(self.Histo_Croisements_Niveaux.loc[F1 | F2].sort_values(by=['Prix'], ascending=False))'''
            
            #Nouveaux niveaux à trader :
            #print('   Recherche des nouveaux niveaux adjacents')
            self.NiveauxATrader = self.NiveauDuJour.get_Niveaux_adjacents_a_trader(self.prix_courant, self.ParamDistanceTriggers)
            self.nom_niveau_trading_sup = self.NiveauxATrader.loc['Sup+','Nivo']
            self.nom_niveau_trading_inf = self.NiveauxATrader.loc['Inf-','Nivo']
            self.niveau_trading_sup     = self.NiveauxATrader.loc['Sup+','Prix']
            self.niveau_trading_inf     = self.NiveauxATrader.loc['Inf-','Prix']
            self.trigger_sup    = self.NiveauxATrader.loc['Sup+','Trigger']
            self.trigger_inf    = self.NiveauxATrader.loc['Inf-','Trigger']
            
            self.NivAdj = self.NiveauDuJour.get_Niveaux_adjacents(self.prix_courant)
            self.nom_niveau_sup = self.NivAdj.loc['Sup+','Nivo']
            self.nom_niveau_inf = self.NivAdj.loc['Inf-','Nivo']
            self.niveau_sup     = self.NivAdj.loc['Sup+','Prix']
            self.niveau_inf     = self.NivAdj.loc['Inf-','Prix']

            print('------------------------------------------------------------------------------------------------')
            print(ts, "NumTick:", self.numTick, ' Croisement niveau tradable', nomNiveau, " - NOUVEAUX NIVEAUX A TRADER           ---")
            print('------------------------------------------------------------------------------------------------')
            print(self.NiveauxATrader)
            print('Dernier prix ',price)
            print('------------------------------------------------------------------------------------------------')
            
            if self.NiveauRisquePeriodeCourante == "OK"  and self.NiveauRisqueVolumesActuels == "OK":
                print(ts,'Initialisation positions...')
                self.init_positions()
            '''
            elif self.NiveauRisquePeriodeCourante == "KO" :
                print(ts, "Période actuelle non favorable au trading (heure) jusqu'à {:s}. Attente fin période risquée...".format(self.HorairesDuJour.get_next_heure_OK_pour_trading()))
            elif self.NiveauRisqueVolumesActuels == "KO":
                print(ts, "Période actuelle non favorable au trading (débit). Attente fin période risquée...")
            '''
            

        # Croisement des niveaux adjacents inf et sup non tradables : historisation
        if (( self.prix_courant > self.niveau_sup and self.niveau_sup !=  self.niveau_trading_sup) 
           or (self.prix_courant < self.niveau_inf and self.niveau_inf !=  self.niveau_trading_inf) ):

            #Historisation de ce croisement 
            if self.prix_courant > self.niveau_sup:
                nomNiveau  = self.nom_niveau_sup
                prix = self.niveau_sup
                '''
                print("================================================================================================================================")
                print(ts, " - tickByTickAllLast - croisement à la hausse niveau non tradable {:s} ({:0.2F}), prix actuel : {:0.2F}".format(nomNiveau, prix, price))
                print("================================================================================================================================")
                '''
            else:
                nomNiveau  = self.nom_niveau_inf
                prix = self.niveau_inf
                '''
                print("================================================================================================================================")
                print(ts, " - tickByTickAllLast - croisement à la baisse niveau non tradable {:s} ({:0.2F}), prix actuel : {:0.2F}".format(nomNiveau, prix, price))
                print("================================================================================================================================")
                '''

            #print('   Historisation du croisement du niveau ', nomNiveau, 'NumTick=', self.numTick)
            HeureStr = datetime.datetime.fromtimestamp(time).strftime("%H:%M:%S")
            Order_ID0   = self.Histo_Croisements_Niveaux.loc[nomNiveau,'OrderID']
            numTickAntePrec = self.Histo_Croisements_Niveaux.loc[nomNiveau,'numTickPrec']
            tsAntePrec = self.Histo_Croisements_Niveaux.loc[nomNiveau,'TSPrec']
            self.Histo_Croisements_Niveaux.loc[nomNiveau]=[prix, tsAntePrec, numTickAntePrec, HeureStr, self.numTick, Order_ID0]

            F1=(self.Histo_Croisements_Niveaux['numTickPrec']>0)
            F2=(self.Histo_Croisements_Niveaux['OrderID']>0)
            '''print(" === Liste des niveaux déjà croisés === ")
            print(self.Histo_Croisements_Niveaux.loc[F1 | F2].sort_values(by=['Prix'], ascending=False))'''

            self.NivAdj = self.NiveauDuJour.get_Niveaux_adjacents(self.prix_courant)
            self.nom_niveau_sup = self.NivAdj.loc['Sup+','Nivo']
            self.nom_niveau_inf = self.NivAdj.loc['Inf-','Nivo']
            self.niveau_sup     = self.NivAdj.loc['Sup+','Prix']
            self.niveau_inf     = self.NivAdj.loc['Inf-','Prix']

        
        # Affichage des ordres ouverts tous les R ticks
        R = 1000
        self.numPasTicks=self.numTick % R
        if self.numPasTicks==0:
            print(ts, "numTick: ", self.numTick, "Prix actuel:", price, 
                  " - Demande affichage des positions ouvertes actuelles")
            self.demande_open_orders=True
            print('-------------------------------------------------')
            print('---       Liste des Ordres                    ---')
            print('-------------------------------------------------')
            print(self.Histo_Mes_Ordres)
            print('Dernier prix ',price)
            print('-------------------------------------------------')
            
            # Sauvegarde des ordres passés / annulés dans la journée :
            self.Histo_Mes_Ordres.to_csv(FichierHistoOrdresJour,sep=';',decimal=',',float_format='%.1f')
            
            self.reqOpenOrders()


    # ! [tickbytickalllast]            

    def get_points_pivots(self, option:str, bar: BarData):
        
        if option == 'getHisto':
            
            self.Histo_ohlc_5min = pd.DataFrame(columns=['Date','open','high','low','close','Volume'])
            self.Histo_ohlc_5min.set_index("Date", inplace=True)
            
            
            #On est obligé de répcupérer les agrégats Quotidiens, hebdo et mensuels, pour avoir les settle
            #le settle ne correspond pas exactement au close, ce sont 2 choses différentes. 
            #Le settle est un prix moyen calculé sur une plage de cotation définie qui dépend 
            #du sous jacent traité (dax, cac, Dow...)
            ts = datetime.datetime.today()

            FinDuMoisDernier        = (ts - datetime.timedelta(days=ts.day)).strftime("%Y%m%d") + " 23:00:00"  

            # Ecart avec le vendredi de la semaine derniere :
            NumDay = datetime.datetime.today().weekday()
            if NumDay > 4 :
                NbDaysAgo=NumDay-4
            else:
                NbDaysAgo=NumDay+3

            FinDeSemaineDerniere    = (ts - datetime.timedelta(days=NbDaysAgo)).strftime("%Y%m%d") + " 23:00:00"  
            
            # Ecart avec le dernier jour ouvré :
            #Si jour courant = lundi, on récupère jusquà vendredi d'avant : 3j avant
            #Sinon on récupère la veille : 1j avant
            NumDay = datetime.datetime.today().weekday()
            if NumDay == 0:
                NbDaysAgo=3
            else:
                NbDaysAgo=1
            
            JourPrec = (ts - datetime.timedelta(days=NbDaysAgo)).strftime("%Y%m%d")  + " 23:00:00"         

            print(ts,"get_points_pivots - Option getHisto => appel requete reqHistoricalData...")
            #queryTime = (datetime.datetime.today()).strftime("%Y%m%d %H:%M:%S")
            self.reqHistoricalData(4103, self.contract, JourPrec,
                              "1 D", "1 day", "TRADES", 0, 1, False, [])
            self.reqHistoricalData(4104, self.contract, FinDeSemaineDerniere,
                               "1 W", "1 week", "TRADES", 0, 1, False, [])
            self.reqHistoricalData(4105, self.contract, FinDuMoisDernier,
                               "1 M", "1 month", "TRADES", 0, 1, False, [])

        if option == 'AjouterDf':
            
            #date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d')
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.Histo_ohlc_5min.loc[date_ts]=[bar.open, bar.high, bar.low, bar.close, bar.volume]
            
        if option == 'Calculer':

            #auj = datetime.datetime.today() - datetime.timedelta(days=0)
            #J_Cour = auj.strftime('%Y-%m-%d')
            ts = datetime.datetime.fromtimestamp(tm.time())
            m5=(ts.minute // 5)*5
            ts5min=datetime.datetime(ts.year,ts.month,ts.day,ts.hour,m5)
            self.Histo_ohlc_5min.loc[ts5min]=[0,0, 0, 0, 1]

            #print("TEST TEST")
            #print(self.Histo_ohlc_5min)

            self.Histo_ohlc_5min.reset_index(inplace=True)
            self.Histo_ohlc_5min['semaine']=self.Histo_ohlc_5min['Date'].apply(lambda x: pd.Timestamp(x).strftime('A%Y-S%U'))
            self.Histo_ohlc_5min.sort_values(by=['Date'], ascending=True, inplace=True)
            #print(self.Histo_ohlc_5min)
           
            
            self.Histo_ohlc_5min.set_index("Date", inplace=True)    
            #print(self.Histo_ohlc_5min)
            #********************************************#
            #   Calcul des points pivots QUOTIDENS:
            #********************************************#
            
            resampled=self.Histo_ohlc_5min.groupby(pd.Grouper(freq='1d'))
            dfQ=self.get_bar_stats(resampled)
            
            dfQ.rename(columns={ 0: 'S4',1:'mS4',2:'S3',3:'mS3',4:'S2',5:'mS2',6:'S1',7:'mS1',8:'Piv',9:'mR1',10:'R1'}, inplace=True)
            dfQ.rename(columns={ 11: 'mR2',12:'R2',13:'mR3',14:'R3',15:'mR4',16:'R4'}, inplace=True)
            
            #Suppression des lignes correspondant aux WE, introduites par l'aggregation 1d :
            dfQ=dfQ[dfQ['vol']>0]
            
            #Shift des points pivots sur les bonnes journées :
            dfQ.sort_values(by=['Date'],inplace=True)
            dfQ['S4'] =dfQ['S4'].shift(1)
            dfQ['mS4']=dfQ['mS4'].shift(1)
            dfQ['S3'] =dfQ['S3'].shift(1)
            dfQ['mS3']=dfQ['mS3'].shift(1)
            dfQ['S2'] =dfQ['S2'].shift(1)
            dfQ['mS2']=dfQ['mS2'].shift(1)
            dfQ['S1'] =dfQ['S1'].shift(1)
            dfQ['mS1']=dfQ['mS1'].shift(1)
            dfQ['Piv']=dfQ['Piv'].shift(1)
            dfQ['mR1']=dfQ['mR1'].shift(1)
            dfQ['R1'] =dfQ['R1'].shift(1)
            dfQ['mR2']=dfQ['mR2'].shift(1)
            dfQ['R2'] =dfQ['R2'].shift(1)
            dfQ['mR3']=dfQ['mR3'].shift(1)
            dfQ['R3'] =dfQ['R3'].shift(1)
            dfQ['mR4']=dfQ['mR4'].shift(1)
            dfQ['R4'] =dfQ['R4'].shift(1)
            dfQ['Haut(J,Pre)']=dfQ['high'].shift(1)
            dfQ['Bas(J,Pre)'] =dfQ['low'].shift(1)
            
            dfQ.rename(columns={ 'S4': 'S4J','mS4':'mS4J','S3':'S3J','mS3':'mS3J','S2':'S2J','mS2':'mS2J','S1':'S1J','mS1':'mS1J','Piv':'PivJ','mR1':'mR1J','R1':'R1J'}, inplace=True)
            dfQ.rename(columns={ 'mR2': 'mR2J','R2':'R2J','mR3':'mR3J','R3':'R3J','mR4':'mR4J','R4':'R4J'}, inplace=True)
            
            dfQ.reset_index(inplace=True)    
            dfQ['semaine']=dfQ['Date'].apply(lambda x: pd.Timestamp(x).strftime('A%Y-S%U'))
            print(dfQ)



            #********************************************#
            #   Calcul des points pivots HEBDOS:
            #********************************************#
            resampled=self.Histo_ohlc_5min.groupby(pd.Grouper(key='semaine'))
            dfH=self.get_bar_stats(resampled)
            
            dfH.rename(columns={ 0: 'S4',1:'mS4',2:'S3',3:'mS3',4:'S2',5:'mS2',6:'S1',7:'mS1',8:'Piv',9:'mR1',10:'R1'}, inplace=True)
            dfH.rename(columns={ 11: 'mR2',12:'R2',13:'mR3',14:'R3',15:'mR4',16:'R4'}, inplace=True)
            
            #Suppression des lignes correspondant aux WE, introduites par l'aggregation 1d :
            dfH=dfH[dfH['vol']>0]
            
            #Shift des points pivots sur les bonnes journées :
            dfH.sort_values(by=['semaine'],inplace=True)
            dfH['S4'] =dfH['S4'].shift(1)
            dfH['mS4']=dfH['mS4'].shift(1)
            dfH['S3'] =dfH['S3'].shift(1)
            dfH['mS3']=dfH['mS3'].shift(1)
            dfH['S2'] =dfH['S2'].shift(1)
            dfH['mS2']=dfH['mS2'].shift(1)
            dfH['S1'] =dfH['S1'].shift(1)
            dfH['mS1']=dfH['mS1'].shift(1)
            dfH['Piv']=dfH['Piv'].shift(1)
            dfH['mR1']=dfH['mR1'].shift(1)
            dfH['R1'] =dfH['R1'].shift(1)
            dfH['mR2']=dfH['mR2'].shift(1)
            dfH['R2'] =dfH['R2'].shift(1)
            dfH['mR3']=dfH['mR3'].shift(1)
            dfH['R3'] =dfH['R3'].shift(1)
            dfH['mR4']=dfH['mR4'].shift(1)
            dfH['R4'] =dfH['R4'].shift(1)
            dfH['Haut(S,Pre)']=dfH['high'].shift(1)
            dfH['Bas(S,Pre)'] =dfH['low'].shift(1)
            
            dfH.rename(columns={ 'S4': 'S4S','mS4':'mS4S','S3':'S3S','mS3':'mS3S','S2':'S2S','mS2':'mS2S','S1':'S1S','mS1':'mS1S','Piv':'PivS','mR1':'mR1S','R1':'R1S'}, inplace=True)
            dfH.rename(columns={ 'mR2': 'mR2S','R2':'R2S','mR3':'mR3S','R3':'R3S','mR4':'mR4S','R4':'R4S'}, inplace=True)
            
            dfH.reset_index(inplace=True)
            #dfH['semaine']=dfH['Date'].apply(lambda x: pd.Timestamp(x).strftime('A%Y-S%U'))
            #dfH.drop(columns=['Date'],inplace=True)

            print(dfH)

            #********************************************#
            #   Calcul des points pivots MENSUELS:
            #********************************************#
            resampled=self.Histo_ohlc_5min.groupby(pd.Grouper(freq='1m'))
            dfM=self.get_bar_stats(resampled)
            
            dfM.rename(columns={ 0: 'S4',1:'mS4',2:'S3',3:'mS3',4:'S2',5:'mS2',6:'S1',7:'mS1',8:'Piv',9:'mR1',10:'R1'}, inplace=True)
            dfM.rename(columns={ 11: 'mR2',12:'R2',13:'mR3',14:'R3',15:'mR4',16:'R4'}, inplace=True)
            
            #Suppression des lignes correspondant aux WE, introduites par l'aggregation 1d :
            dfM=dfM[dfM['vol']>0]
            
            #Shift des points pivots sur les bonnes journées :
            dfM.sort_values(by=['Date'],inplace=True)
            dfM['S4'] =dfM['S4'].shift(1)
            dfM['mS4']=dfM['mS4'].shift(1)
            dfM['S3'] =dfM['S3'].shift(1)
            dfM['mS3']=dfM['mS3'].shift(1)
            dfM['S2'] =dfM['S2'].shift(1)
            dfM['mS2']=dfM['mS2'].shift(1)
            dfM['S1'] =dfM['S1'].shift(1)
            dfM['mS1']=dfM['mS1'].shift(1)
            dfM['Piv']=dfM['Piv'].shift(1)
            dfM['mR1']=dfM['mR1'].shift(1)
            dfM['R1'] =dfM['R1'].shift(1)
            dfM['mR2']=dfM['mR2'].shift(1)
            dfM['R2'] =dfM['R2'].shift(1)
            dfM['mR3']=dfM['mR3'].shift(1)
            dfM['R3'] =dfM['R3'].shift(1)
            dfM['mR4']=dfM['mR4'].shift(1)
            dfM['R4'] =dfM['R4'].shift(1)
            dfM['Haut(M,Pre)']=dfM['high'].shift(1)
            dfM['Bas(M,Pre)'] =dfM['low'].shift(1)
            
            dfM.rename(columns={ 'S4': 'S4M','mS4':'mS4M','S3':'S3M','mS3':'mS3M','S2':'S2M','mS2':'mS2M','S1':'S1M','mS1':'mS1M','Piv':'PivM','mR1':'mR1M','R1':'R1M'}, inplace=True)
            dfM.rename(columns={ 'mR2': 'mR2M','R2':'R2M','mR3':'mR3M','R3':'R3M','mR4':'mR4M','R4':'R4M'}, inplace=True)
            
            
            dfM.reset_index(inplace=True)
            dfM['mois']=dfM['Date'].apply(lambda x: pd.Timestamp(x).strftime('A%Y-M%m'))
            dfM.drop(columns=['Date'],inplace=True)

            print(dfM)

            #********************************************#
            #   Points pivot pour aujourd'hui :
            #********************************************#
            auj = datetime.datetime.today() - datetime.timedelta(days=0)
            J_Cour = auj.strftime('%Y-%m-%d')
            S_Cour = auj.strftime('A%Y-S%U')
            M_Cour = auj.strftime('A%Y-M%m')
         
            NiveauxCalcules = pd.DataFrame(columns=['Nom','Prix'])
            
            F=(dfQ['Date']==J_Cour)
            df=dfQ.loc[F]
            for i in range(6,25,1):
                NiveauxCalcules=NiveauxCalcules.append(pd.DataFrame([[df.iloc[:,i].name,df.iloc[:,i].values[0]]],columns=['Nom','Prix']),ignore_index=True)

            F=(dfH['semaine']==S_Cour)
            df=dfH.loc[F]
            for i in range(6,25,1):
                NiveauxCalcules=NiveauxCalcules.append(pd.DataFrame([[df.iloc[:,i].name,df.iloc[:,i].values[0]]],columns=['Nom','Prix']),ignore_index=True)

            F=(dfM['mois']==M_Cour)
            df=dfM.loc[F]
            for i in range(5,24,1):
                NiveauxCalcules=NiveauxCalcules.append(pd.DataFrame([[df.iloc[:,i].name,df.iloc[:,i].values[0]]],columns=['Nom','Prix']),ignore_index=True)

            #print("**********************************")
            #print(Niveaux)

            #Sauvegarde sous forme de fichier csv:
            #DateStr = datetime.datetime.today().strftime("%Y-%m-%d")
            #FichierNiveauxDuJour = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Dax mini\ NiveauxCalcules-' + DateStr + '.csv'
            NiveauxCalcules.to_csv(FichierNiveauxDuJour,sep=';', index=False)    
                
            self.NiveauDuJour=Niveaux.Niveaux()
            self.NiveauDuJour.Init_Niveaux(NiveauxCalcules, self.seuilFusionNiveaux, NomFichierNiveauxEnrichi)
                        
        if option == 'CalculerAvecFichier':
            
            #DateStr = datetime.datetime.today().strftime("%Y-%m-%d")
            #FichierNiveauxDuJour = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Dax mini\ NiveauxCalcules-' + DateStr + '.csv'
            
            NiveauxCalcules = pd.read_csv(FichierNiveauxDuJour,sep=';')

            self.NiveauDuJour=Niveaux.Niveaux()
            self.NiveauDuJour.Init_Niveaux(NiveauxCalcules, self.seuilFusionNiveaux, NomFichierNiveauxEnrichi)
                                          
            
    def get_bar_stats(self, agg_trades):
        #vwap=agg_trades.apply(lambda x: np.ma.average(x.Prix,weights=x.NbLots)).to_frame('vwap')
        open=agg_trades.open.first()
        close=agg_trades.close.last()
        low=agg_trades.low.min()
        high=agg_trades.high.max()
        vol=agg_trades.Volume.sum().to_frame('vol')
        #H=ohlc['high'].shift(1)
        #L=ohlc['low'].shift(1)
        #C=ohlc['close'].shift(1)
        H=high
        L=low
        C=close
        Pivot=(H+L+C)/3
        R1=2*Pivot-L
        R2=Pivot+(H-L)
        R3=H + 2*(Pivot-L)
        R4=R3 + (H-L)
        S1=2*Pivot-H
        S2=Pivot-(H-L)
        S3=L - 2*(H-Pivot)
        S4=S3 - (H-L)
        mS4=(S4+S3)/2
        mS3=(S3+S2)/2
        mS2=(S2+S1)/2
        mS1=(S1+Pivot)/2
        mR4=(R4+R3)/2
        mR3=(R3+R2)/2
        mR2=(R2+R1)/2
        mR1=(R1+Pivot)/2
        return pd.concat([open, high, low, close,vol,S4,mS4,S3,mS3,S2,mS2,S1,mS1,Pivot,mR1,R1,mR2,R2,mR3,R3,mR4,R4],axis=1)
            


    @iswrapper
    # ! [tickbytickbidask]
    def tickByTickBidAsk(self, reqId: int, time: int, bidPrice: float, askPrice: float,
                         bidSize: int, askSize: int, tickAttribBidAsk: TickAttribBidAsk):
        super().tickByTickBidAsk(reqId, time, bidPrice, askPrice, bidSize,
                                 askSize, tickAttribBidAsk)
        print("BidAsk. ReqId:", reqId,
              "Time:", datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S"),
              "BidPrice:", bidPrice, "AskPrice:", askPrice, "BidSize:", bidSize,
              "AskSize:", askSize, "BidPastLow:", tickAttribBidAsk.bidPastLow, "AskPastHigh:", tickAttribBidAsk.askPastHigh)
    # ! [tickbytickbidask]

    # ! [tickbytickmidpoint]
    @iswrapper
    def tickByTickMidPoint(self, reqId: int, time: int, midPoint: float):
        super().tickByTickMidPoint(reqId, time, midPoint)
        print("Midpoint. ReqId:", reqId,
              "Time:", datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S"),
              "MidPoint:", midPoint)
    # ! [tickbytickmidpoint]

    @printWhenExecuting
    def marketDepthOperations_req(self):
        # Requesting the Deep Book
        # ! [reqmarketdepth]
        self.reqMktDepth(2001, ContractSamples.EurGbpFx(), 5, False, [])
        # ! [reqmarketdepth]

        # ! [reqmarketdepth]
        self.reqMktDepth(2002, ContractSamples.EuropeanStock(), 5, True, [])
        # ! [reqmarketdepth]

        # Request list of exchanges sending market depth to UpdateMktDepthL2()
        # ! [reqMktDepthExchanges]
        self.reqMktDepthExchanges()
        # ! [reqMktDepthExchanges]

    @printWhenExecuting
    def marketDepthOperations_cancel(self):
        # Canceling the Deep Book request
        # ! [cancelmktdepth]
        self.cancelMktDepth(2001, False)
        self.cancelMktDepth(2002, True)
        # ! [cancelmktdepth]

    @iswrapper
    # ! [updatemktdepth]
    def updateMktDepth(self, reqId: TickerId, position: int, operation: int,
                       side: int, price: float, size: int):
        super().updateMktDepth(reqId, position, operation, side, price, size)
        print("UpdateMarketDepth. ReqId:", reqId, "Position:", position, "Operation:",
              operation, "Side:", side, "Price:", price, "Size:", size)
    # ! [updatemktdepth]

    @iswrapper
    # ! [updatemktdepthl2]
    def updateMktDepthL2(self, reqId: TickerId, position: int, marketMaker: str,
                         operation: int, side: int, price: float, size: int, isSmartDepth: bool):
        super().updateMktDepthL2(reqId, position, marketMaker, operation, side,
                                 price, size, isSmartDepth)
        print("UpdateMarketDepthL2. ReqId:", reqId, "Position:", position, "MarketMaker:", marketMaker, "Operation:",
              operation, "Side:", side, "Price:", price, "Size:", size, "isSmartDepth:", isSmartDepth)

    # ! [updatemktdepthl2]

    @iswrapper
    # ! [rerouteMktDepthReq]
    def rerouteMktDepthReq(self, reqId: int, conId: int, exchange: str):
        super().rerouteMktDataReq(reqId, conId, exchange)
        print("Re-route market depth request. ReqId:", reqId, "ConId:", conId, "Exchange:", exchange)
    # ! [rerouteMktDepthReq]

    @printWhenExecuting
    def realTimeBarsOperations_req(self):
        # Requesting real time bars
        # ! [reqrealtimebars]
        self.reqRealTimeBars(3001, ContractSamples.EurGbpFx(), 5, "MIDPOINT", True, [])
        # ! [reqrealtimebars]

    @iswrapper
    # ! [realtimebar]
    def realtimeBar(self, reqId: TickerId, time:int, open_: float, high: float, low: float, close: float,
                        volume: int, wap: float, count: int):
        super().realtimeBar(reqId, time, open_, high, low, close, volume, wap, count)
        print("RealTimeBar. TickerId:", reqId, RealTimeBar(time, -1, open_, high, low, close, volume, wap, count))
    # ! [realtimebar]

    @printWhenExecuting
    def realTimeBarsOperations_cancel(self):
        # Canceling real time bars
        # ! [cancelrealtimebars]
        self.cancelRealTimeBars(3001)
        # ! [cancelrealtimebars]

    @printWhenExecuting
    def historicalDataOperations_req(self):
        # Requesting historical data
        # ! [reqHeadTimeStamp]
        self.reqHeadTimeStamp(4101, ContractSamples.USStockAtSmart(), "TRADES", 0, 1)
        # ! [reqHeadTimeStamp]

        # ! [reqhistoricaldata]
        queryTime = (datetime.datetime.today() - datetime.timedelta(days=180)).strftime("%Y%m%d %H:%M:%S")
        self.reqHistoricalData(4102, ContractSamples.EurGbpFx(), queryTime,
                               "1 M", "1 day", "MIDPOINT", 1, 1, False, [])
        self.reqHistoricalData(4103, ContractSamples.EuropeanStock(), queryTime,
                               "10 D", "1 min", "TRADES", 1, 1, False, [])
        self.reqHistoricalData(4104, ContractSamples.EurGbpFx(), "",
                               "1 M", "1 day", "MIDPOINT", 1, 1, True, [])
        # ! [reqhistoricaldata]

    @printWhenExecuting
    def historicalDataOperations_cancel(self):
        # ! [cancelHeadTimestamp]
        self.cancelHeadTimeStamp(4101)
        # ! [cancelHeadTimestamp]
        
        # Canceling historical data requests
        # ! [cancelhistoricaldata]
        self.cancelHistoricalData(4102)
        self.cancelHistoricalData(4103)
        self.cancelHistoricalData(4104)
        # ! [cancelhistoricaldata]

    @printWhenExecuting
    def historicalTicksOperations(self):
        # ! [reqhistoricalticks]
        self.reqHistoricalTicks(18001, ContractSamples.USStockAtSmart(),
                                "20170712 21:39:33", "", 10, "TRADES", 1, True, [])
        self.reqHistoricalTicks(18002, ContractSamples.USStockAtSmart(),
                                "20170712 21:39:33", "", 10, "BID_ASK", 1, True, [])
        self.reqHistoricalTicks(18003, ContractSamples.USStockAtSmart(),
                                "20170712 21:39:33", "", 10, "MIDPOINT", 1, True, [])
        # ! [reqhistoricalticks]

    @iswrapper
    # ! [headTimestamp]
    def headTimestamp(self, reqId:int, headTimestamp:str):
        print("HeadTimestamp. ReqId:", reqId, "HeadTimeStamp:", headTimestamp)
    # ! [headTimestamp]

    @iswrapper
    # ! [histogramData]
    def histogramData(self, reqId:int, items:HistogramDataList):
        print("HistogramData. ReqId:", reqId, "HistogramDataList:", "[%s]" % "; ".join(map(str, items)))
    # ! [histogramData]

    @iswrapper
    # ! [historicaldata]
    def historicalData(self, reqId:int, bar: BarData):
        #print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        if self.debutReceptionFlux5min:
            ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
            print(ts, "reqId:", reqId, "bar:", bar)
            #self.debutReceptionFlux5min = False      
        #self.get_points_pivots("AjouterDf", bar)
    # ! [historicaldata]

    @iswrapper
    # ! [historicaldataend]
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print("Fin réception flux en 5 minutes - HistoricalDataEnd - ReqId:", reqId, "from", start, "to", end)
        #self.get_points_pivots("Calculer", None)
        
        # Activation du flux tick by tick :
        #self.tickByTickOperations_req()
        
    # ! [historicaldataend]

    @iswrapper
    # ! [historicalDataUpdate]
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        print("HistoricalDataUpdate. ReqId:", reqId, "BarData.", bar)
    # ! [historicalDataUpdate]

    @iswrapper
    # ! [historicalticks]
    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTick, done: bool):
        for tick in ticks:
            print("HistoricalTick. ReqId:", reqId, tick)
    # ! [historicalticks]

    @iswrapper
    # ! [historicalticksbidask]
    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk,
                              done: bool):
        for tick in ticks:
            print("HistoricalTickBidAsk. ReqId:", reqId, tick)
    # ! [historicalticksbidask]

    @iswrapper
    # ! [historicaltickslast] 88888888888
    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast,
                            done: bool):
        tsNow = datetime.datetime.today()
        
        
       
        #print(tsNow, 'historicalTicksLast, reqId:',reqId, "  - Size ticks rendus:",len(ticks))
        for tick in ticks:
            self.tsPrec_dt =  self.ts_dt
            self.ts_dt = datetime.datetime.fromtimestamp(tick.time)
            if self.nbTrades > 1 and self.ts_dt < self.tsPrec_dt :
                print("Arrivée tick non triée, tsPrec:", self.tsPrec_dt,"ts:", self.ts_dt)
            ts = self.ts_dt.strftime("%Y%m%d %H:%M:%S.%f")
            self.nbTicks=self.nbTicks + tick.size 
            self.nbTrades = self.nbTrades + 1
            #print("HistoricalTickLast. ReqId:", reqId, ts,"numTrade:",self.nbTrades, tick)
            self.Histo_ticks.loc[self.nbTrades]=[self.ts_dt, tick.price, tick.size, self.nbTicks]
            self.time_dernier_tick = datetime.datetime.fromtimestamp(tick.time)
        
        TickVideConfirmed = False
        if done and len(ticks) == 0 and self.nbTicksPrec == 0:
            TickVideConfirmed = True
        self.nbTicksPrec = len(ticks)
        
        # print(self.Histo_ticks)
        # ts2 = self.ts_dt.strftime("%Y%m%d %H.%M.%S.%f")
        # FichierHistoTicksJourTs = self.FichierHistoTicksJour + ts2 + '.csv'
        # self.Histo_ticks.to_csv(FichierHistoTicksJourTs,sep=';',decimal='.',float_format='%.1f')
        if len(ticks) > 0 :
            print(tsNow, 'Reponse historicalTicksLast, reqId:',reqId, "- done=", done, "  - Size ticks rendus:",len(ticks),
                  "- 1er / dernier tick rendu:", datetime.datetime.fromtimestamp(ticks[0].time),
                  "-", datetime.datetime.fromtimestamp(ticks[-1].time))
        else:
            print(tsNow, 'Reponse historicalTicksLast, reqId:',reqId, "- done=", done, "  - Size ticks rendus:",len(ticks))

        ts_next_dt = datetime.datetime.strptime(self.ts_next, '%Y%m%d %H:%M:%S') 
        

        if done and len(ticks) == 0 and self.time_dernier_tick < self.ts_fin_du_jour and not TickVideConfirmed:
            self.ts_next = (max(ts_next_dt, self.time_dernier_tick) + datetime.timedelta(seconds=1)).strftime("%Y%m%d %H:%M:%S")
            #print(tsNow, " TS Next:", ts_next)
            self.reqHistoricalTicks(reqId+1, self.contract, self.ts_next, "", NbTicksDemandes, "TRADES", 0, True, []);
        
        elif done and (len(ticks) >= NbTicksDemandes) and self.time_dernier_tick < self.ts_fin_du_jour:
            self.ts_next = (self.time_dernier_tick + datetime.timedelta(seconds=1)).strftime("%Y%m%d %H:%M:%S")
            #print(tsNow, " TS Next:", ts_next)
            self.reqHistoricalTicks(reqId+1, self.contract, self.ts_next, "", NbTicksDemandes, "TRADES", 0, True, []);
        else:
            if done == False:
                #print(tsNow, ' historicalTicksLast, reqId:',reqId, "  - Attente fin traitement requete")
                a=1
            else:
                #Journée complete :
                #☻self.time_dernier_tick = datetime.datetime.fromtimestamp(tick.time)
                print(tsNow, " Flux complet pour jour ", self.DateStr, " - Dernier tick du jour à ",self.time_dernier_tick)
                self.Histo_ticks.to_csv(self.FichierHistoTicksJour,sep=';',decimal='.',float_format='%.1f')
    
                
                #Recherche Journée pas déjà traitée, passage au lendemain:
                JourneeDejaTraiteePourCeContrat = True
                while JourneeDejaTraiteePourCeContrat == True and self.DateCourDt < self.DateFinDt:
                        
                    self.DateCourDt = self.DateCourDt + datetime.timedelta(days=1)
                    self.DateStr = self.DateCourDt.strftime('%Y-%m-%d')
                    Future_NomContrat=ListeContrats[self.contrat_courant][0]
                    Future_EcheanceContrat=ListeContrats[self.contrat_courant][1]
                    repertoire = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\00 - Histo Ticks\\HistoTicks_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat
                    path = repertoire + '\\HistoTicks_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat + "_Q" 
                    self.FichierHistoTicksJour    =   path + self.DateStr + '.csv'
                    self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
                    self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
                    self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')
    
                    #Si fichier déjà existant, on passe à la journée suivante :
                    try:
                        with open(self.FichierHistoTicksJour): 
                            print(tsNow, ' Journée déjà traitée:', self.DateStr)
                            JourneeDejaTraiteePourCeContrat = True
                    except IOError:
                            JourneeDejaTraiteePourCeContrat = False
    
                if JourneeDejaTraiteePourCeContrat == False:
                    self.nbTicks = 0
                    self.nbTrades = 0
                    self.Histo_ticks = self.Histo_ticks.drop(self.Histo_ticks.index)
                    print("\n", tsNow, ' ===========1 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                    #print(self.Histo_ticks)
                    print(tsNow, ' appel   reqHistoricalTicks, reqId:',reqId+1, self.TsDebutStr)
                    self.reqHistoricalTicks(reqId+1, self.contract, self.TsDebutStr, "", NbTicksDemandes, "TRADES", 0, True, []);
                else:
                    print(tsNow, " fin reception flux de toutes les journées demandées pour ce contrat...")
    
                    
                    #Passage au contrat suivant si pas tous déjà faits :
                    JourneeDejaTraiteePourCeContrat = True
                    while JourneeDejaTraiteePourCeContrat == True and self.contrat_courant < len(self.ListeContrats) - 1:
                        self.contrat_courant = self.contrat_courant + 1
                        Future_NomContrat=ListeContrats[self.contrat_courant][0]
                        Future_EcheanceContrat=ListeContrats[self.contrat_courant][1]
                        print(tsNow, " ----------------------")
                        print(tsNow, " Contrat suivant : " + Future_NomContrat + "- Ech"+ Future_EcheanceContrat)
                        print(tsNow, " ----------------------")
                        self.contract = self.create_contract(Future_NomContrat, Future_EcheanceContrat)  # Create a contract
     
                        
                        repertoire = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\00 - Histo Ticks\\HistoTicks_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat
                        path = repertoire + '\\HistoTicks_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat + "_Q" 
            
                        if not os.path.exists(repertoire):
                            os.makedirs(repertoire)
                     
    
                        self.DateStr    = DateStr
                        self.DateDebDt  = DateDebDt
                        self.DateFinDt  = DateFinDt
                        
                        self.tsPrec_dt =  None
                        self.ts_dt =  None
    
                        self.DateCourDt =  self.DateDebDt
                        self.FichierHistoTicksJour    =   path + self.DateStr + '.csv'
                        self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
                        self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
                        self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')
         
                        self.nbTicks = 0
                        self.nbTrades = 0
                        self.Histo_ticks = self.Histo_ticks.drop(self.Histo_ticks.index)
                        #print('=========== ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                        #print(self.Histo_ticks)
    
                        #Si fichier déjà existant, on passe à la journée suivante :
                        try:
                            with open(self.FichierHistoTicksJour): 
                                print(tsNow, " Journée déjà traitée:", self.DateStr)
                                JourneeDejaTraiteePourCeContrat = True
                        except IOError:
                                JourneeDejaTraiteePourCeContrat = False
    
                        #Recherche Journée pas déjà traitée, passage au lendemain:
                        while JourneeDejaTraiteePourCeContrat == True and self.DateCourDt < self.DateFinDt:
                                
                            self.DateCourDt = self.DateCourDt + datetime.timedelta(days=1)
                            self.DateStr = self.DateCourDt.strftime('%Y-%m-%d')
                            self.FichierHistoTicksJour    =   path + self.DateStr + '.csv'
                            self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
                            self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
                            self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')
    
                            #Si fichier déjà existant, on passe à la journée suivante :
                            try:
                                with open(self.FichierHistoTicksJour): 
                                    print(tsNow, ' Journée déjà traitée:', self.DateStr)
                                    JourneeDejaTraiteePourCeContrat = True
                            except IOError:
                                    JourneeDejaTraiteePourCeContrat = False
    
                        if JourneeDejaTraiteePourCeContrat == False:
                            self.nbTicks = 0
                            self.nbTrades = 0
                            self.Histo_ticks = self.Histo_ticks.drop(self.Histo_ticks.index)
                            print(tsNow, ' ===========2 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                            #print(self.Histo_ticks)
                            print(tsNow, ' appel   reqHistoricalTicks, reqId:',reqId+1, self.TsDebutStr)
                            self.reqHistoricalTicks(reqId+1, self.contract, self.TsDebutStr, "", NbTicksDemandes, "TRADES", 0, True, []);
                        else:
                            print(tsNow, " fin reception flux de toutes les journées demandées pour ce contrat...")
    
                            
                        
                     
                    print(tsNow, " fin traitement de tous les contrats..")
                    #self.stop()
                
            


    # ! [historicaltickslast]

    @printWhenExecuting
    def optionsOperations_req(self):
        # ! [reqsecdefoptparams]
        self.reqSecDefOptParams(0, "IBM", "", "STK", 8314)
        # ! [reqsecdefoptparams]

        # Calculating implied volatility
        # ! [calculateimpliedvolatility]
        self.calculateImpliedVolatility(5001, ContractSamples.OptionAtBOX(), 5, 85, [])
        # ! [calculateimpliedvolatility]

        # Calculating option's price
        # ! [calculateoptionprice]
        self.calculateOptionPrice(5002, ContractSamples.OptionAtBOX(), 0.22, 85, [])
        # ! [calculateoptionprice]

        # Exercising options
        # ! [exercise_options]
        self.exerciseOptions(5003, ContractSamples.OptionWithTradingClass(), 1,
                             1, self.account, 1)
        # ! [exercise_options]

    @printWhenExecuting
    def optionsOperations_cancel(self):
        # Canceling implied volatility
        self.cancelCalculateImpliedVolatility(5001)
        # Canceling option's price calculation
        self.cancelCalculateOptionPrice(5002)

    @iswrapper
    # ! [securityDefinitionOptionParameter]
    def securityDefinitionOptionParameter(self, reqId: int, exchange: str,
                                          underlyingConId: int, tradingClass: str, multiplier: str,
                                          expirations: SetOfString, strikes: SetOfFloat):
        super().securityDefinitionOptionParameter(reqId, exchange,
                                                  underlyingConId, tradingClass, multiplier, expirations, strikes)
        print("SecurityDefinitionOptionParameter.",
              "ReqId:", reqId, "Exchange:", exchange, "Underlying conId:", underlyingConId, "TradingClass:", tradingClass, "Multiplier:", multiplier,
              "Expirations:", expirations, "Strikes:", str(strikes))
    # ! [securityDefinitionOptionParameter]

    @iswrapper
    # ! [securityDefinitionOptionParameterEnd]
    def securityDefinitionOptionParameterEnd(self, reqId: int):
        super().securityDefinitionOptionParameterEnd(reqId)
        print("SecurityDefinitionOptionParameterEnd. ReqId:", reqId)
    # ! [securityDefinitionOptionParameterEnd]

    @iswrapper
    # ! [tickoptioncomputation]
    def tickOptionComputation(self, reqId: TickerId, tickType: TickType,
                              impliedVol: float, delta: float, optPrice: float, pvDividend: float,
                              gamma: float, vega: float, theta: float, undPrice: float):
        super().tickOptionComputation(reqId, tickType, impliedVol, delta,
                                      optPrice, pvDividend, gamma, vega, theta, undPrice)
        print("TickOptionComputation. TickerId:", reqId, "TickType:", tickType,
              "ImpliedVolatility:", impliedVol, "Delta:", delta, "OptionPrice:",
              optPrice, "pvDividend:", pvDividend, "Gamma: ", gamma, "Vega:", vega,
              "Theta:", theta, "UnderlyingPrice:", undPrice)

    # ! [tickoptioncomputation]


    @printWhenExecuting
    def contractOperations(self):
        # ! [reqcontractdetails]
        self.reqContractDetails(210, ContractSamples.OptionForQuery())
        self.reqContractDetails(211, ContractSamples.EurGbpFx())
        self.reqContractDetails(212, ContractSamples.Bond())
        self.reqContractDetails(213, ContractSamples.FuturesOnOptions())
        self.reqContractDetails(214, ContractSamples.SimpleFuture())
        # ! [reqcontractdetails]

        # ! [reqmatchingsymbols]
        self.reqMatchingSymbols(211, "IB")
        # ! [reqmatchingsymbols]

    @printWhenExecuting
    def newsOperations_req(self):
        # Requesting news ticks
        # ! [reqNewsTicks]
        self.reqMktData(10001, ContractSamples.USStockAtSmart(), "mdoff,292", False, False, []);
        # ! [reqNewsTicks]

        # Returns list of subscribed news providers
        # ! [reqNewsProviders]
        self.reqNewsProviders()
        # ! [reqNewsProviders]

        # Returns body of news article given article ID
        # ! [reqNewsArticle]
        self.reqNewsArticle(10002,"BRFG", "BRFG$04fb9da2", [])
        # ! [reqNewsArticle]

        # Returns list of historical news headlines with IDs
        # ! [reqHistoricalNews]
        self.reqHistoricalNews(10003, 8314, "BRFG", "", "", 10, [])
        # ! [reqHistoricalNews]

        # ! [reqcontractdetailsnews]
        self.reqContractDetails(10004, ContractSamples.NewsFeedForQuery())
        # ! [reqcontractdetailsnews]

    @printWhenExecuting
    def newsOperations_cancel(self):
        # Canceling news ticks
        # ! [cancelNewsTicks]
        self.cancelMktData(10001);
        # ! [cancelNewsTicks]

    @iswrapper
    #! [tickNews]
    def tickNews(self, tickerId: int, timeStamp: int, providerCode: str,
                 articleId: str, headline: str, extraData: str):
        print("TickNews. TickerId:", tickerId, "TimeStamp:", timeStamp,
              "ProviderCode:", providerCode, "ArticleId:", articleId,
              "Headline:", headline, "ExtraData:", extraData)
    #! [tickNews]

    @iswrapper
    #! [historicalNews]
    def historicalNews(self, reqId: int, time: str, providerCode: str,
                       articleId: str, headline: str):
        print("HistoricalNews. ReqId:", reqId, "Time:", time,
              "ProviderCode:", providerCode, "ArticleId:", articleId,
              "Headline:", headline)
    #! [historicalNews]

    @iswrapper
    #! [historicalNewsEnd]
    def historicalNewsEnd(self, reqId:int, hasMore:bool):
        print("HistoricalNewsEnd. ReqId:", reqId, "HasMore:", hasMore)
    #! [historicalNewsEnd]

    @iswrapper
    #! [newsProviders]
    def newsProviders(self, newsProviders: ListOfNewsProviders):
        print("NewsProviders: ")
        for provider in newsProviders:
            print("NewsProvider.", provider)
    #! [newsProviders]

    @iswrapper
    #! [newsArticle]
    def newsArticle(self, reqId: int, articleType: int, articleText: str):
        print("NewsArticle. ReqId:", reqId, "ArticleType:", articleType,
              "ArticleText:", articleText)
    #! [newsArticle]

    @iswrapper
    # ! [contractdetails]
    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        printinstance(contractDetails)
    # ! [contractdetails]

    @iswrapper
    # ! [bondcontractdetails]
    def bondContractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().bondContractDetails(reqId, contractDetails)
        printinstance(contractDetails)
    # ! [bondcontractdetails]

    @iswrapper
    # ! [contractdetailsend]
    def contractDetailsEnd(self, reqId: int):
        super().contractDetailsEnd(reqId)
        print("ContractDetailsEnd. ReqId:", reqId)
    # ! [contractdetailsend]

    @iswrapper
    # ! [symbolSamples]
    def symbolSamples(self, reqId: int,
                      contractDescriptions: ListOfContractDescription):
        super().symbolSamples(reqId, contractDescriptions)
        print("Symbol Samples. Request Id: ", reqId)

        for contractDescription in contractDescriptions:
            derivSecTypes = ""
            for derivSecType in contractDescription.derivativeSecTypes:
                derivSecTypes += derivSecType
                derivSecTypes += " "
            print("Contract: conId:%s, symbol:%s, secType:%s primExchange:%s, "
                  "currency:%s, derivativeSecTypes:%s" % (
                contractDescription.contract.conId,
                contractDescription.contract.symbol,
                contractDescription.contract.secType,
                contractDescription.contract.primaryExchange,
                contractDescription.contract.currency, derivSecTypes))
    # ! [symbolSamples]

    @printWhenExecuting
    def marketScannersOperations_req(self):
        # Requesting list of valid scanner parameters which can be used in TWS
        # ! [reqscannerparameters]
        self.reqScannerParameters()
        # ! [reqscannerparameters]

        # Triggering a scanner subscription
        # ! [reqscannersubscription]
        self.reqScannerSubscription(7001, ScannerSubscriptionSamples.HighOptVolumePCRatioUSIndexes(), [], [])

        # Generic Filters
        tagvalues = []
        tagvalues.append(TagValue("usdMarketCapAbove", "10000"))
        tagvalues.append(TagValue("optVolumeAbove", "1000"))
        tagvalues.append(TagValue("avgVolumeAbove", "10000"));

        self.reqScannerSubscription(7002, ScannerSubscriptionSamples.HotUSStkByVolume(), [], tagvalues) # requires TWS v973+
        # ! [reqscannersubscription]

        # ! [reqcomplexscanner]
        AAPLConIDTag = [TagValue("underConID", "265598")]
        self.reqScannerSubscription(7003, ScannerSubscriptionSamples.ComplexOrdersAndTrades(), [], AAPLConIDTag) # requires TWS v975+
        
        # ! [reqcomplexscanner]


    @printWhenExecuting
    def marketScanners_cancel(self):
        # Canceling the scanner subscription
        # ! [cancelscannersubscription]
        self.cancelScannerSubscription(7001)
        self.cancelScannerSubscription(7002)
        self.cancelScannerSubscription(7003)
        # ! [cancelscannersubscription]

    @iswrapper
    # ! [scannerparameters]
    def scannerParameters(self, xml: str):
        super().scannerParameters(xml)
        open('log/scanner.xml', 'w').write(xml)
        print("ScannerParameters received.")
    # ! [scannerparameters]

    @iswrapper
    # ! [scannerdata]
    def scannerData(self, reqId: int, rank: int, contractDetails: ContractDetails,
                    distance: str, benchmark: str, projection: str, legsStr: str):
        super().scannerData(reqId, rank, contractDetails, distance, benchmark,
                            projection, legsStr)
#        print("ScannerData. ReqId:", reqId, "Rank:", rank, "Symbol:", contractDetails.contract.symbol,
#              "SecType:", contractDetails.contract.secType,
#              "Currency:", contractDetails.contract.currency,
#              "Distance:", distance, "Benchmark:", benchmark,
#              "Projection:", projection, "Legs String:", legsStr)
        print("ScannerData. ReqId:", reqId, ScanData(contractDetails.contract, rank, distance, benchmark, projection, legsStr))
    # ! [scannerdata]

    @iswrapper
    # ! [scannerdataend]
    def scannerDataEnd(self, reqId: int):
        super().scannerDataEnd(reqId)
        print("ScannerDataEnd. ReqId:", reqId)
        # ! [scannerdataend]

    @iswrapper
    # ! [smartcomponents]
    def smartComponents(self, reqId:int, smartComponentMap:SmartComponentMap):
        super().smartComponents(reqId, smartComponentMap)
        print("SmartComponents:")
        for smartComponent in smartComponentMap:
            print("SmartComponent.", smartComponent)
    # ! [smartcomponents]

    @iswrapper
    # ! [tickReqParams]
    def tickReqParams(self, tickerId:int, minTick:float,
                      bboExchange:str, snapshotPermissions:int):
        super().tickReqParams(tickerId, minTick, bboExchange, snapshotPermissions)
        print("TickReqParams. TickerId:", tickerId, "MinTick:", minTick,
              "BboExchange:", bboExchange, "SnapshotPermissions:", snapshotPermissions)
    # ! [tickReqParams]

    @iswrapper
    # ! [mktDepthExchanges]
    def mktDepthExchanges(self, depthMktDataDescriptions:ListOfDepthExchanges):
        super().mktDepthExchanges(depthMktDataDescriptions)
        print("MktDepthExchanges:")
        for desc in depthMktDataDescriptions:
            print("DepthMktDataDescription.", desc)
    # ! [mktDepthExchanges]

    @printWhenExecuting
    def fundamentalsOperations_req(self):
        # Requesting Fundamentals
        # ! [reqfundamentaldata]
        self.reqFundamentalData(8001, ContractSamples.USStock(), "ReportsFinSummary", [])
        # ! [reqfundamentaldata]
        
        # ! [fundamentalexamples]
        self.reqFundamentalData(8002, ContractSamples.USStock(), "ReportSnapshot", []); # for company overview
        self.reqFundamentalData(8003, ContractSamples.USStock(), "ReportRatios", []); # for financial ratios
        self.reqFundamentalData(8004, ContractSamples.USStock(), "ReportsFinStatements", []); # for financial statements
        self.reqFundamentalData(8005, ContractSamples.USStock(), "RESC", []); # for analyst estimates
        self.reqFundamentalData(8006, ContractSamples.USStock(), "CalendarReport", []); # for company calendar
        # ! [fundamentalexamples]

    @printWhenExecuting
    def fundamentalsOperations_cancel(self):
        # Canceling fundamentalsOperations_req request
        # ! [cancelfundamentaldata]
        self.cancelFundamentalData(8001)
        # ! [cancelfundamentaldata]

        # ! [cancelfundamentalexamples]
        self.cancelFundamentalData(8002)
        self.cancelFundamentalData(8003)
        self.cancelFundamentalData(8004)
        self.cancelFundamentalData(8005)
        self.cancelFundamentalData(8006)
        # ! [cancelfundamentalexamples]

    @iswrapper
    # ! [fundamentaldata]
    def fundamentalData(self, reqId: TickerId, data: str):
        super().fundamentalData(reqId, data)
        print("FundamentalData. ReqId:", reqId, "Data:", data)
    # ! [fundamentaldata]

    @printWhenExecuting
    def bulletinsOperations_req(self):
        # Requesting Interactive Broker's news bulletinsOperations_req
        # ! [reqnewsbulletins]
        self.reqNewsBulletins(True)
        # ! [reqnewsbulletins]

    @printWhenExecuting
    def bulletinsOperations_cancel(self):
        # Canceling IB's news bulletinsOperations_req
        # ! [cancelnewsbulletins]
        self.cancelNewsBulletins()
        # ! [cancelnewsbulletins]

    @iswrapper
    # ! [updatenewsbulletin]
    def updateNewsBulletin(self, msgId: int, msgType: int, newsMessage: str,
                           originExch: str):
        super().updateNewsBulletin(msgId, msgType, newsMessage, originExch)
        print("News Bulletins. MsgId:", msgId, "Type:", msgType, "Message:", newsMessage,
              "Exchange of Origin: ", originExch)
        # ! [updatenewsbulletin]

    def ocaSample(self):
        # OCA ORDER
        # ! [ocasubmit]
        ocaOrders = [OrderSamples.LimitOrder("BUY", 1, 10), OrderSamples.LimitOrder("BUY", 1, 11),
                     OrderSamples.LimitOrder("BUY", 1, 12)]
        OrderSamples.OneCancelsAll("TestOCA_" + str(self.nextValidOrderId), ocaOrders, 2)
        for o in ocaOrders:
            self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), o)
            # ! [ocasubmit]

    def conditionSamples(self):
        # ! [order_conditioning_activate]
        mkt = OrderSamples.MarketOrder("BUY", 100)
        # Order will become active if conditioning criteria is met
        mkt.conditions.append(
            OrderSamples.PriceCondition(PriceCondition.TriggerMethodEnum.Default,
                                        208813720, "SMART", 600, False, False))
        mkt.conditions.append(OrderSamples.ExecutionCondition("EUR.USD", "CASH", "IDEALPRO", True))
        mkt.conditions.append(OrderSamples.MarginCondition(30, True, False))
        mkt.conditions.append(OrderSamples.PercentageChangeCondition(15.0, 208813720, "SMART", True, True))
        mkt.conditions.append(OrderSamples.TimeCondition("20160118 23:59:59", True, False))
        mkt.conditions.append(OrderSamples.VolumeCondition(208813720, "SMART", False, 100, True))
        self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), mkt)
        # ! [order_conditioning_activate]

        # Conditions can make the order active or cancel it. Only LMT orders can be conditionally canceled.
        # ! [order_conditioning_cancel]
        lmt = OrderSamples.LimitOrder("BUY", 100, 20)
        # The active order will be cancelled if conditioning criteria is met
        lmt.conditionsCancelOrder = True
        lmt.conditions.append(
            OrderSamples.PriceCondition(PriceCondition.TriggerMethodEnum.Last,
                                        208813720, "SMART", 600, False, False))
        self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), lmt)
        # ! [order_conditioning_cancel]

    def bracketSample(self):
        # BRACKET ORDER
        # ! [bracketsubmit]
        bracket = OrderSamples.BracketOrder(self.nextOrderId(), "BUY", 100, 30, 40, 20)
        for o in bracket:
            self.placeOrder(o.orderId, ContractSamples.EuropeanStock(), o)
            self.nextOrderId()  # need to advance this we'll skip one extra oid, it's fine
            # ! [bracketsubmit]

    def hedgeSample(self):
        # F Hedge order
        # ! [hedgesubmit]
        # Parent order on a contract which currency differs from your base currency
        parent = OrderSamples.LimitOrder("BUY", 100, 10)
        parent.orderId = self.nextOrderId()
        parent.transmit = False
        # Hedge on the currency conversion
        hedge = OrderSamples.MarketFHedge(parent.orderId, "BUY")
        # Place the parent first...
        self.placeOrder(parent.orderId, ContractSamples.EuropeanStock(), parent)
        # Then the hedge order
        self.placeOrder(self.nextOrderId(), ContractSamples.EurGbpFx(), hedge)
        # ! [hedgesubmit]

    def algoSamples(self):
        # ! [scale_order]
        scaleOrder = OrderSamples.RelativePeggedToPrimary("BUY",  70000,  189,  0.01);
        AvailableAlgoParams.FillScaleParams(scaleOrder, 2000, 500, True, .02, 189.00, 3600, 2.00, True, 10, 40);
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), scaleOrder);
        # ! [scale_order]

        tm.sleep(1)

        # ! [algo_base_order]
        baseOrder = OrderSamples.LimitOrder("BUY", 1000, 1)
        # ! [algo_base_order]

        # ! [arrivalpx]
        AvailableAlgoParams.FillArrivalPriceParams(baseOrder, 0.1, "Aggressive", "09:00:00 CET", "16:00:00 CET", True, True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [arrivalpx]

        # ! [darkice]
        AvailableAlgoParams.FillDarkIceParams(baseOrder, 10, "09:00:00 CET", "16:00:00 CET", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [darkice]

        # ! [place_midprice]
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), OrderSamples.Midprice("BUY", 1, 150))
        # ! [place_midprice]
		
        # ! [ad]
        # The Time Zone in "startTime" and "endTime" attributes is ignored and always defaulted to GMT
        AvailableAlgoParams.FillAccumulateDistributeParams(baseOrder, 10, 60, True, True, 1, True, True, "20161010-12:00:00 GMT", "20161010-16:00:00 GMT")
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [ad]

        # ! [twap]
        AvailableAlgoParams.FillTwapParams(baseOrder, "Marketable", "09:00:00 CET", "16:00:00 CET", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [twap]

        # ! [vwap]
        AvailableAlgoParams.FillVwapParams(baseOrder, 0.2, "09:00:00 CET", "16:00:00 CET", True, True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [vwap]

        # ! [balanceimpactrisk]
        AvailableAlgoParams.FillBalanceImpactRiskParams(baseOrder, 0.1, "Aggressive", True)
        self.placeOrder(self.nextOrderId(), ContractSamples.USOptionContract(), baseOrder)
        # ! [balanceimpactrisk]

        # ! [minimpact]
        AvailableAlgoParams.FillMinImpactParams(baseOrder, 0.3)
        self.placeOrder(self.nextOrderId(), ContractSamples.USOptionContract(), baseOrder)
        # ! [minimpact]

        # ! [adaptive]
        AvailableAlgoParams.FillAdaptiveParams(baseOrder, "Normal")
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [adaptive]

        # ! [closepx]
        AvailableAlgoParams.FillClosePriceParams(baseOrder, 0.4, "Neutral", "20180926-06:06:49", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [closepx]

        # ! [pctvol]
        AvailableAlgoParams.FillPctVolParams(baseOrder, 0.5, "12:00:00 EST", "14:00:00 EST", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [pctvol]

        # ! [pctvolpx]
        AvailableAlgoParams.FillPriceVariantPctVolParams(baseOrder, 0.1, 0.05, 0.01, 0.2, "12:00:00 EST", "14:00:00 EST", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [pctvolpx]

        # ! [pctvolsz]
        AvailableAlgoParams.FillSizeVariantPctVolParams(baseOrder, 0.2, 0.4, "12:00:00 EST", "14:00:00 EST", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [pctvolsz]

        # ! [pctvoltm]
        AvailableAlgoParams.FillTimeVariantPctVolParams(baseOrder, 0.2, 0.4, "12:00:00 EST", "14:00:00 EST", True, 100000)
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), baseOrder)
        # ! [pctvoltm]

        # ! [jeff_vwap_algo]
        AvailableAlgoParams.FillJefferiesVWAPParams(baseOrder, "10:00:00 EST", "16:00:00 EST", 10, 10, "Exclude_Both", 130, 135, 1, 10, "Patience", False, "Midpoint")
        self.placeOrder(self.nextOrderId(), ContractSamples.JefferiesContract(), baseOrder)
        # ! [jeff_vwap_algo]

        # ! [csfb_inline_algo]
        AvailableAlgoParams.FillCSFBInlineParams(baseOrder, "10:00:00 EST", "16:00:00 EST", "Patient", 10, 20, 100, "Default", False, 40, 100, 100, 35)
        self.placeOrder(self.nextOrderId(), ContractSamples.CSFBContract(), baseOrder)
        # ! [csfb_inline_algo]

        # ! [qbalgo_strobe_algo]
        AvailableAlgoParams.FillQBAlgoInLineParams(baseOrder, "10:00:00 EST", "16:00:00 EST", -99, "TWAP", 0.25, True)
        self.placeOrder(self.nextOrderId(), ContractSamples.QBAlgoContract(), baseOrder)
        # ! [qbalgo_strobe_algo]

    @printWhenExecuting
    def financialAdvisorOperations(self):
        # Requesting FA information
        # ! [requestfaaliases]
        self.requestFA(FaDataTypeEnum.ALIASES)
        # ! [requestfaaliases]

        # ! [requestfagroups]
        self.requestFA(FaDataTypeEnum.GROUPS)
        # ! [requestfagroups]

        # ! [requestfaprofiles]
        self.requestFA(FaDataTypeEnum.PROFILES)
        # ! [requestfaprofiles]

        # Replacing FA information - Fill in with the appropriate XML string.
        # ! [replacefaonegroup]
        self.replaceFA(FaDataTypeEnum.GROUPS, FaAllocationSamples.FaOneGroup)
        # ! [replacefaonegroup]

        # ! [replacefatwogroups]
        self.replaceFA(FaDataTypeEnum.GROUPS, FaAllocationSamples.FaTwoGroups)
        # ! [replacefatwogroups]

        # ! [replacefaoneprofile]
        self.replaceFA(FaDataTypeEnum.PROFILES, FaAllocationSamples.FaOneProfile)
        # ! [replacefaoneprofile]

        # ! [replacefatwoprofiles]
        self.replaceFA(FaDataTypeEnum.PROFILES, FaAllocationSamples.FaTwoProfiles)
        # ! [replacefatwoprofiles]

        # ! [reqSoftDollarTiers]
        self.reqSoftDollarTiers(14001)
        # ! [reqSoftDollarTiers]

    @iswrapper
    # ! [receivefa]
    def receiveFA(self, faData: FaDataType, cxml: str):
        super().receiveFA(faData, cxml)
        print("Receiving FA: ", faData)
        open('log/fa.xml', 'w').write(cxml)
    # ! [receivefa]

    @iswrapper
    # ! [softDollarTiers]
    def softDollarTiers(self, reqId: int, tiers: list):
        super().softDollarTiers(reqId, tiers)
        print("SoftDollarTiers. ReqId:", reqId)
        for tier in tiers:
            print("SoftDollarTier.", tier)
    # ! [softDollarTiers]

    @printWhenExecuting
    def miscelaneousOperations(self):
        # Request TWS' current time
        self.reqCurrentTime()
        # Setting TWS logging level
        self.setServerLogLevel(1)

    @printWhenExecuting
    def linkingOperations(self):
        # ! [querydisplaygroups]
        self.queryDisplayGroups(19001)
        # ! [querydisplaygroups]

        # ! [subscribetogroupevents]
        self.subscribeToGroupEvents(19002, 1)
        # ! [subscribetogroupevents]

        # ! [updatedisplaygroup]
        self.updateDisplayGroup(19002, "8314@SMART")
        # ! [updatedisplaygroup]

        # ! [subscribefromgroupevents]
        self.unsubscribeFromGroupEvents(19002)
        # ! [subscribefromgroupevents]

    @iswrapper
    # ! [displaygrouplist]
    def displayGroupList(self, reqId: int, groups: str):
        super().displayGroupList(reqId, groups)
        print("DisplayGroupList. ReqId:", reqId, "Groups", groups)
    # ! [displaygrouplist]

    @iswrapper
    # ! [displaygroupupdated]
    def displayGroupUpdated(self, reqId: int, contractInfo: str):
        super().displayGroupUpdated(reqId, contractInfo)
        print("DisplayGroupUpdated. ReqId:", reqId, "ContractInfo:", contractInfo)
    # ! [displaygroupupdated]

    @printWhenExecuting
    def whatIfOrderOperations(self):
    # ! [whatiflimitorder]
        whatIfOrder = OrderSamples.LimitOrder("SELL", 5, 70)
        whatIfOrder.whatIf = True
        self.placeOrder(self.nextOrderId(), ContractSamples.USStockAtSmart(), whatIfOrder)
    # ! [whatiflimitorder]
        tm.sleep(2)

    # Stratégie S1 : entrée en position à 4 pts au-dessous du pivot, sortie à 1 pt    
    def OpenPositionLongue_S1_req(self, nomNiveau, prix, nbLots):


        if self.NiveauRisquePeriodeCourante == "KO":
            print('      Période risque elevé : on ne positionne pas de nouvel ordre')
            return

        if self.NiveauRisqueVolumesActuels == "KO":
            print('      Période risque elevé cause volumes inhabituels : on ne positionne pas de nouvel ordre')
            return


        if self.Histo_Croisements_Niveaux.loc[nomNiveau,'OrderID'] > 0:
            print('      Ordre déjà positionné sur ce niveau ', nomNiveau)
            return
        
        NbTicksDepuisCrossTrigger = self.numTick - self.Histo_Croisements_Triggers_L.loc[nomNiveau,'TickDernierCross']
        if NbTicksDepuisCrossTrigger > self.DureeVieOrdreParent_Ticks:
            print("      Trigger croisé depuis trop longtemps. Situation probable de range juste au-dessus du niveau.", end='')
            print(" Le trigger d'achat sur le niveau {:s} a été croisé il y a {:0F} ticks (max autorisé={:0F})".format(nomNiveau, 
                                                                                                                       NbTicksDepuisCrossTrigger, 
                                                                                                                       self.DureeVieOrdreParent_Ticks))
            return


        ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
        print("   ", ts, " - OpenPositionLongue_S1_req ", nomNiveau, "prix achat:", 
              prix, "Nb lots:", nbLots, " NumTick:", self.numTick)

        #Sur le dax, on ne peut pas trader les valeurs décimales
        if prix % 1 > 0: #il y a une partie decimale non nulle
            prix=int(prix) # on arrondit à la valeur entière inférieure
            print("      Modif prix achat : nouveau prix conforme au contrat:", prix)
            
        #Il faut tout d'abord vérifier que ce niveau n'a pas déjà été franchi récemment:
        print('      Verification validité du niveau ', nomNiveau)
 
        if self.Histo_Croisements_Niveaux.loc[nomNiveau,'TSPrec'] != None:
            ts_now  = datetime.datetime.today()    
            ts_prec_str = ts_now.strftime("%Y%m%d ")  + self.Histo_Croisements_Niveaux.loc[nomNiveau,'TSPrec']
            ts_prec  = datetime.datetime.strptime(ts_prec_str, '%Y%m%d  %H:%M:%S')
            delta_TS = ts_now - ts_prec
            delta_minutes = int(round(delta_TS.total_seconds() / 60))
    
            if delta_minutes < self.duree_minimum_avant_reactivation_niveau:
                print('         Niveau invalide, déjà traversé il y a ', delta_minutes, 
                      ' minutes (moins que le seuil requis de ', self.duree_minimum_avant_reactivation_niveau,
                      ')')
                print('         Ouverture de position avortée') 
                return 

        delta_Ticks = self.numTick - self.Histo_Croisements_Niveaux.loc[nomNiveau,'numTickPrec']
        if delta_Ticks < self.nb_ticks_minimum_avant_reactivation_niveau:
            print('         Niveau invalide, déjà traversé il y a ', delta_Ticks, 
                  ' ticks (moins que le seuil requis de', self.nb_ticks_minimum_avant_reactivation_niveau,
                  ')')
            print('         Ouverture de position avortée') 
        
        else:
            print('         Niveau valide') 

            print('      verification coherence prix achat :') 
            if prix > self.prix_courant:
                print("         PROBLEME COHERENCE PRIX ACHAT. Le prix d'achat ( ", 
                      prix, ') est superieur au prix courant du marche (', 
                      self.prix_courant, ")") 
            else:
                print("         Coherence prix achat OK par rapport au prix actuel du marche ") 
                print("         Prix achat :", prix, ' - Prix courant du marche : ', 
                      self.prix_courant)  

                print('      Placement des 3 ordres de la stratégie S1 :') 
    
                # ! [order_submission]
                self.simplePlaceOid = self.nextOrderId()
                ID1=self.simplePlaceOid 
                print('         Placement ordre principal   BUY,   prix = ', prix, ', nb lots = ', nbLots, "ID = ", ID1) 
                print('         Placement ordre Take Profit SELL , prix = ', prix+self.stop_profit_S1, ', nb lots = ', nbLots) 
                print('         Placement ordre Stop Loss   SELL , prix = ', prix-self.stop_loss_S1, ', nb lots = ', nbLots) 
               
                bracket = OrderSamples.BracketOrder(ID1, "BUY", nbLots, 
                                                    prix, prix+self.stop_profit_S1, prix-self.stop_loss_S1)
                for o in bracket:
                   self.placeOrder(o.orderId, self.contract, o)
                   self.nextOrderId()  # need to advance this we'll skip one extra oid, it's fine
                   # ! [bracketsubmit]

                #print("   On garde une trace de l'ouverture de la position")
                TickValidite = self.Histo_Croisements_Triggers_L.loc[nomNiveau,'TickDernierCross'] + self.DureeVieOrdreParent_Ticks
                self.Histo_Croisements_Niveaux.loc[nomNiveau,'OrderID']=ID1
                ts_heure = datetime.datetime.fromtimestamp(tm.time()).strftime("%H:%M:%S")
                self.Histo_Mes_Ordres.loc[ID1]  =[nomNiveau, "Parent",     "BUY",  prix,                     nbLots, TickValidite, "Demandé", 0, nbLots, 0, 0, ts_heure]
                self.Histo_Mes_Ordres.loc[ID1+1]=[nomNiveau, "TakeProfit", "SELL", prix+self.stop_profit_S1, nbLots, TickValidite, "Demandé", 0, nbLots, 0, 0, ts_heure]
                self.Histo_Mes_Ordres.loc[ID1+2]=[nomNiveau, "StopLoss",   "SELL", prix-self.stop_loss_S1,   nbLots, TickValidite, "Demandé", 0, nbLots, 0, 0, ts_heure]
                
                print("***** Histo_Mes_Ordres 2 *****")
                print( self.Histo_Mes_Ordres)

                
                
                '''
                self.placeOrder(self.simplePlaceOid, self.contract,
                                OrderSamples.LimitOrder('BUY', nbLots, prix))
         
                
                print('   Placement ordre SELL Take Profit, prix = ', prix+self.stop_profit_S1, 'nb lots = ', nbLots) 
                self.simplePlaceOid = self.nextOrderId()
                ID2=self.simplePlaceOid 
                self.placeOrder(self.simplePlaceOid, self.contract,
                                OrderSamples.LimitOrder('SELL', nbLots, prix+self.stop_profit_S1))
         
                print('   Placement ordre SELL Stop Loss, prix = ', prix-self.stop_loss_S1, 'nb lots = ', nbLots) 
                self.simplePlaceOid = self.nextOrderId()
                ID3=self.simplePlaceOid 
                self.placeOrder(self.simplePlaceOid, self.contract,
                                OrderSamples.Stop('SELL', nbLots, prix-self.stop_loss_S1))
                
                #On stocke les liens entre les ordres passés :
                self.ordres_lies.loc[ID1]=[ID2,ID3]
                self.ordres_lies.loc[ID2]=ID3
                self.ordres_lies.loc[ID3]=ID2  '''


        #TODO : Lier les ordres de SELL dans une structure de type dataframe, pour pouvoir canceler une patte quand l'autre s'est bien exécutée
        
 
    # Stratégie S1 : entrée en position à 4 pts au-dessus du pivot, sortie à 1 pt      
    def OpenPositionCourte_S1_req(self, nomNiveau, prix, nbLots):

        ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
 
        if self.NiveauRisquePeriodeCourante == "KO":
            print('      Période risque elevé : on ne positionne pas de nouvel ordre')
            return 

        if self.NiveauRisqueVolumesActuels == "KO":
            print('      Période risque elevé cause volumes inhabituels : on ne positionne pas de nouvel ordre')
            return 

        if self.Histo_Croisements_Niveaux.loc[nomNiveau,'OrderID'] > 0:
            print('      Ordre déjà positionné sur ce niveau ', nomNiveau)
            return 

        NbTicksDepuisCrossTrigger = self.numTick - self.Histo_Croisements_Triggers_H.loc[nomNiveau,'TickDernierCross']
        if NbTicksDepuisCrossTrigger > self.DureeVieOrdreParent_Ticks:
            print("      Trigger croisé depuis trop longtemps. Situation probable de range juste au-dessous du niveau.", end='')
            print(" Le trigger de vente sous le niveau {:s} a été croisé il y a {:0F} ticks (max autorisé={:0F})".format(nomNiveau, 
                                                                                                                       NbTicksDepuisCrossTrigger, 
                                                                                                                       self.DureeVieOrdreParent_Ticks))
            return

        print("      ",ts, " - OpenPositionCourte_S1_req ", nomNiveau, "prix vente:", 
              prix, "Nb lots:", nbLots, " NumTick:", self.numTick)

        #Sur le dax, on ne peut pas trader les valeurs décimales
        if prix % 1 > 0: #il y a une partie decimale non nulle
            prix=int(prix)+1 # on arrondit à la valeur entière supérieure
            print("      Modif prix vente : nouveau prix conforme au contrat:", prix)


        #Il faut vérifier que ce niveau n'a pas déjà été franchi récemment:
        print('      Verification validité du niveau ', nomNiveau)
 
        if self.Histo_Croisements_Niveaux.loc[nomNiveau,'TSPrec'] != None:
            ts_now  = datetime.datetime.today()    
            ts_prec_str = ts_now.strftime("%Y%m%d ")  + self.Histo_Croisements_Niveaux.loc[nomNiveau,'TSPrec']
            ts_prec  = datetime.datetime.strptime(ts_prec_str, '%Y%m%d  %H:%M:%S')
            delta_TS = ts_now - ts_prec
            delta_minutes = int(round(delta_TS.total_seconds() / 60))
    
            if delta_minutes < self.duree_minimum_avant_reactivation_niveau:
                print('         Niveau invalide, déjà traversé il y a ', delta_minutes, 
                      ' minutes (moins que le seuil requis de ', self.duree_minimum_avant_reactivation_niveau,
                      ')')
                print('         Ouverture de position avortée') 
                return 


        delta_Ticks = self.numTick - self.Histo_Croisements_Niveaux.loc[nomNiveau,'numTickPrec']
        if delta_Ticks < self.nb_ticks_minimum_avant_reactivation_niveau:
            print('         Niveau invalide, déjà traversé il y a ', delta_Ticks, 
                  ' ticks (moins que le suil requis de ', self.nb_ticks_minimum_avant_reactivation_niveau,
                  ')')
            print('         Ouverture de position avortée') 
            return 
        
        else:
            print('         Niveau valide') 

            print('      verification coherence prix vente :') 
            if prix < self.prix_courant:
                print("         PROBLEME COHERENCE PRIX VENTE. Le prix de vente ( ", 
                      prix, ') est inferieur au prix courant du marche (', 
                      self.prix_courant, ")") 
                return 
            else:
                print("         Coherence prix vente OK par rapport au prix actuel du marche ") 
                print("         Prix vente :", prix, ' - Prix courant du marche : ', 
                      self.prix_courant)  

                print('   Placement des 3 ordres de la stratégie S1 :') 
                
                # ! [order_submission]
                self.simplePlaceOid = self.nextOrderId()
                ID1=self.simplePlaceOid 
                print('         Placement ordre principal   SELL, prix = ', prix, ', nb lots = ', nbLots, "ID = ", ID1) 
                print('         Placement ordre Take Profit BUY,  prix = ', prix-self.stop_profit_S1, ', nb lots = ', nbLots) 
                print('         Placement ordre Stop Loss   BUY,  prix = ', prix+self.stop_loss_S1, ', nb lots = ', nbLots) 
               
                bracket = OrderSamples.BracketOrder(ID1, "SELL", nbLots, 
                                                    prix, prix-self.stop_profit_S1, prix+self.stop_loss_S1)
                for o in bracket:
                   self.placeOrder(o.orderId, self.contract, o)
                   self.nextOrderId()  # need to advance this we'll skip one extra oid, it's fine
                   # ! [bracketsubmit]

                #print("   On garde une trace de l'ouverture de la position")
                TickValidite = self.Histo_Croisements_Triggers_H.loc[nomNiveau,'TickDernierCross'] + self.DureeVieOrdreParent_Ticks
                self.Histo_Croisements_Niveaux.loc[nomNiveau,'OrderID']=ID1
                ts_heure = datetime.datetime.fromtimestamp(tm.time()).strftime("%H:%M:%S")
                self.Histo_Mes_Ordres.loc[ID1]  =[nomNiveau, "Parent",     "SELL", prix,                     nbLots, TickValidite, "Demandé", 0, nbLots, 0, 0, ts_heure]
                self.Histo_Mes_Ordres.loc[ID1+1]=[nomNiveau, "TakeProfit", "BUY",  prix-self.stop_profit_S1, nbLots, TickValidite, "Demandé", 0, nbLots, 0, 0, ts_heure]
                self.Histo_Mes_Ordres.loc[ID1+2]=[nomNiveau, "StopLoss",   "BUY",  prix+self.stop_loss_S1,   nbLots, TickValidite, "Demandé", 0, nbLots, 0, 0, ts_heure]
                
                print("***** Histo_Mes_Ordres 3 *****")
                print( self.Histo_Mes_Ordres)
                
                return 
                                

                ''' 
                self.placeOrder(self.simplePlaceOid, self.contract,
                                OrderSamples.LimitOrder('SELL', nbLots, prix))
         
                
                print('   Placement ordre BUY Take Profit, prix = ', prix-self.stop_profit_S1, 'nb lots = ', nbLots) 
                self.simplePlaceOid = self.nextOrderId()
                ID2=self.simplePlaceOid 
                self.placeOrder(self.simplePlaceOid, self.contract,
                                OrderSamples.LimitOrder('BUY', nbLots, prix-self.stop_profit_S1))
         
                print('   Placement ordre BUY Stop Loss, prix = ', prix+self.stop_loss_S1, 'nb lots = ', nbLots) 
                self.simplePlaceOid = self.nextOrderId()
                ID3=self.simplePlaceOid 
                self.placeOrder(self.simplePlaceOid, self.contract,
                                OrderSamples.Stop('BUY', nbLots, prix+self.stop_loss_S1))
        
                
                #On stocke les liens entre les ordres passés :
                self.ordres_lies.loc[ID1]=[ID2,ID3]
                self.ordres_lies.loc[ID2]=ID3
                self.ordres_lies.loc[ID3]=ID2 '''
        
  

    @printWhenExecuting
    def orderOperations_req(self):
        # Requesting the next valid id
        # ! [reqids]
        # The parameter is always ignored.
        #self.reqIds(-1)
        # ! [reqids]

        # Requesting all open orders
        # ! [reqallopenorders]
        #self.reqAllOpenOrders()
        # ! [reqallopenorders]

        # Taking over orders to be submitted via TWS
        # ! [reqautoopenorders]
        #self.reqAutoOpenOrders(True)
        # ! [reqautoopenorders]

        # Requesting this API client's orders
        # ! [reqopenorders]
        #self.reqOpenOrders()
        # ! [reqopenorders]


        # Placing/modifying an order - remember to ALWAYS increment the
        # nextValidId after placing an order so it can be used for the next one!
        # Note if there are multiple clients connected to an account, the
        # order ID must also be greater than all order IDs returned for orders
        # to orderStatus and openOrder to this client.

        # ! [order_submission]
        self.simplePlaceOid = self.nextOrderId()
        self.placeOrder(self.simplePlaceOid, self.contract,
                        self.create_order('BUY', 13830, 1))
        # ! [order_submission]
        '''
        # ! [faorderoneaccount]
        faOrderOneAccount = OrderSamples.MarketOrder("BUY", 100)
        # Specify the Account Number directly
        faOrderOneAccount.account = "DU119915"
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), faOrderOneAccount)
        # ! [faorderoneaccount]

        # ! [faordergroupequalquantity]
        faOrderGroupEQ = OrderSamples.LimitOrder("SELL", 200, 2000)
        faOrderGroupEQ.faGroup = "Group_Equal_Quantity"
        faOrderGroupEQ.faMethod = "EqualQuantity"
        self.placeOrder(self.nextOrderId(), ContractSamples.SimpleFuture(), faOrderGroupEQ)
        # ! [faordergroupequalquantity]

        # ! [faordergrouppctchange]
        faOrderGroupPC = OrderSamples.MarketOrder("BUY", 0)
        # You should not specify any order quantity for PctChange allocation method
        faOrderGroupPC.faGroup = "Pct_Change"
        faOrderGroupPC.faMethod = "PctChange"
        faOrderGroupPC.faPercentage = "100"
        self.placeOrder(self.nextOrderId(), ContractSamples.EurGbpFx(), faOrderGroupPC)
        # ! [faordergrouppctchange]

        # ! [faorderprofile]
        faOrderProfile = OrderSamples.LimitOrder("BUY", 200, 100)
        faOrderProfile.faProfile = "Percent_60_40"
        self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), faOrderProfile)
        # ! [faorderprofile]

        # ! [modelorder]
        modelOrder = OrderSamples.LimitOrder("BUY", 200, 100)
        modelOrder.account = "DF12345"
        modelOrder.modelCode = "Technology" # model for tech stocks first created in TWS
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), modelOrder)
        # ! [modelorder]

        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(),
                        OrderSamples.Block("BUY", 50, 20))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(),
                         OrderSamples.BoxTop("SELL", 10))
        self.placeOrder(self.nextOrderId(), ContractSamples.FutureComboContract(),
                         OrderSamples.ComboLimitOrder("SELL", 1, 1, False))
        self.placeOrder(self.nextOrderId(), ContractSamples.StockComboContract(),
                          OrderSamples.ComboMarketOrder("BUY", 1, True))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionComboContract(),
                          OrderSamples.ComboMarketOrder("BUY", 1, False))
        self.placeOrder(self.nextOrderId(), ContractSamples.StockComboContract(),
                          OrderSamples.LimitOrderForComboWithLegPrices("BUY", 1, [10, 5], True))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                         OrderSamples.Discretionary("SELL", 1, 45, 0.5))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(),
                          OrderSamples.LimitIfTouched("BUY", 1, 30, 34))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.LimitOnClose("SELL", 1, 34))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.LimitOnOpen("BUY", 1, 35))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.MarketIfTouched("BUY", 1, 30))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                         OrderSamples.MarketOnClose("SELL", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.MarketOnOpen("BUY", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.MarketOrder("SELL", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.MarketToLimit("BUY", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtIse(),
                          OrderSamples.MidpointMatch("BUY", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.MarketToLimit("BUY", 1))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.Stop("SELL", 1, 34.4))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.StopLimit("BUY", 1, 35, 33))
        self.placeOrder(self.nextOrderId(), ContractSamples.SimpleFuture(),
                          OrderSamples.StopWithProtection("SELL", 1, 45))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.SweepToFill("BUY", 1, 35))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.TrailingStop("SELL", 1, 0.5, 30))
        self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
                          OrderSamples.TrailingStopLimit("BUY", 1, 2, 5, 50))
        self.placeOrder(self.nextOrderId(), ContractSamples.USOptionContract(),
                         OrderSamples.Volatility("SELL", 1, 5, 2))

        self.bracketSample()

        self.conditionSamples()

        self.hedgeSample()

        # NOTE: the following orders are not supported for Paper Trading
        # self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), OrderSamples.AtAuction("BUY", 100, 30.0))
        # self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(), OrderSamples.AuctionLimit("SELL", 10, 30.0, 2))
        # self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(), OrderSamples.AuctionPeggedToStock("BUY", 10, 30, 0.5))
        # self.placeOrder(self.nextOrderId(), ContractSamples.OptionAtBOX(), OrderSamples.AuctionRelative("SELL", 10, 0.6))
        # self.placeOrder(self.nextOrderId(), ContractSamples.SimpleFuture(), OrderSamples.MarketWithProtection("BUY", 1))
        # self.placeOrder(self.nextOrderId(), ContractSamples.USStock(), OrderSamples.PassiveRelative("BUY", 1, 0.5))

        # 208813720 (GOOG)
        # self.placeOrder(self.nextOrderId(), ContractSamples.USStock(),
        #    OrderSamples.PeggedToBenchmark("SELL", 100, 33, True, 0.1, 1, 208813720, "ISLAND", 750, 650, 800))

        # STOP ADJUSTABLE ORDERS
        # Order stpParent = OrderSamples.Stop("SELL", 100, 30)
        # stpParent.OrderId = self.nextOrderId()
        # self.placeOrder(stpParent.OrderId, ContractSamples.EuropeanStock(), stpParent)
        # self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), OrderSamples.AttachAdjustableToStop(stpParent, 35, 32, 33))
        # self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), OrderSamples.AttachAdjustableToStopLimit(stpParent, 35, 33, 32, 33))
        # self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), OrderSamples.AttachAdjustableToTrail(stpParent, 35, 32, 32, 1, 0))

        # Order lmtParent = OrderSamples.LimitOrder("BUY", 100, 30)
        # lmtParent.OrderId = self.nextOrderId()
        # self.placeOrder(lmtParent.OrderId, ContractSamples.EuropeanStock(), lmtParent)
        # Attached TRAIL adjusted can only be attached to LMT parent orders.
        # self.placeOrder(self.nextOrderId(), ContractSamples.EuropeanStock(), OrderSamples.AttachAdjustableToTrailAmount(lmtParent, 34, 32, 33, 0.008))
        self.algoSamples()
        
        self.ocaSample()

        # Request the day's executions
        # ! [reqexecutions]
        self.reqExecutions(10001, ExecutionFilter())
        # ! [reqexecutions]
        
        # Requesting completed orders
        # ! [reqcompletedorders]
        self.reqCompletedOrders(False)
        # ! [reqcompletedorders]
        '''

    def orderOperations_cancel(self):
        if self.simplePlaceOid is not None:
            # ! [cancelorder]
            self.cancelOrder(self.simplePlaceOid)
            # ! [cancelorder]
            
        # Cancel all orders for all accounts
        # ! [reqglobalcancel]
        self.reqGlobalCancel()
        # ! [reqglobalcancel]

    def rerouteCFDOperations(self):
        # ! [reqmktdatacfd]
        self.reqMktData(16001, ContractSamples.USStockCFD(), "", False, False, [])
        self.reqMktData(16002, ContractSamples.EuropeanStockCFD(), "", False, False, []);
        self.reqMktData(16003, ContractSamples.CashCFD(), "", False, False, []);
        # ! [reqmktdatacfd]

        # ! [reqmktdepthcfd]
        self.reqMktDepth(16004, ContractSamples.USStockCFD(), 10, False, []);
        self.reqMktDepth(16005, ContractSamples.EuropeanStockCFD(), 10, False, []);
        self.reqMktDepth(16006, ContractSamples.CashCFD(), 10, False, []);
        # ! [reqmktdepthcfd]

    def marketRuleOperations(self):
        self.reqContractDetails(17001, ContractSamples.USStock())
        self.reqContractDetails(17002, ContractSamples.Bond())

        # ! [reqmarketrule]
        self.reqMarketRule(26)
        self.reqMarketRule(239)
        # ! [reqmarketrule]

    @iswrapper
    # ! [execdetails]
    def execDetails(self, reqId: int, contract: Contract, execution: Execution):
        super().execDetails(reqId, contract, execution)
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
        '''
        print('==================================================================================')
        print('                      Package Robot IB.py - EVT execDetails ')
        print('                          ',ts)
        print('==================================================================================')
        '''
        print(timeStr, " - ExecDetails - Id:", execution.orderId, "- ReqId:", reqId, "Symbol:", contract.symbol, "SecType:", contract.secType, "Currency:", contract.currency, execution)
        
        print(timeStr, " - ExecDetails - Id:", execution.orderId, "- NBLots:", execution.cumQty, "- Prix moyen:",execution.avgPrice, 
              "- Dernier débit calculé:", self.DebitCourant)

        # Mise à jour Prix réel exécuté
        self.Histo_Mes_Ordres.loc[execution.orderId,'PrixExec'] = execution.avgPrice
        ts_heure = datetime.datetime.fromtimestamp(tm.time()).strftime("%H:%M:%S")
        self.Histo_Mes_Ordres.loc[execution.orderId,'TsExec'] = ts_heure
        

        '''
        print("Suppression des ordres lies")
        #print('--------------------------------------------')
        #print('              Tables des ordres liés           ')
        #print('--------------------------------------------')
        #print(self.ordres_lies)
        #print('--------------------------------------------')
        print("Recherche des ordres liés à l'ordre ", execution.orderId)
        ID1 = self.ordres_lies.loc[execution.orderId,"ID_Lie_1"]
        ID2 = self.ordres_lies.loc[execution.orderId,"ID_Lie_2"]
        #Si ID1 = ID2, on est soit sur le TakeProfit soit sur le StopLoss : il faut annuler l'ordre lié:
        if ID1==ID2:
            print("   suppression de l'ordre SL ou TP lie : ", ID1)
            #cancel ordre ID1
            self.cancelOrder(ID1)
        else:
            print("   ordre execute = ordre principal => on ne supprime rien")
         '''  
 
    # ! [execdetails]

    @iswrapper
    # ! [execdetailsend]
    def execDetailsEnd(self, reqId: int):
        super().execDetailsEnd(reqId)
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        print(timeStr, "ExecDetailsEnd. ReqId:", reqId)
    # ! [execdetailsend]

    @iswrapper
    # ! [commissionreport]
    def commissionReport(self, commissionReport: CommissionReport):
        super().commissionReport(commissionReport)
        #print("CommissionReport.", commissionReport)
    # ! [commissionreport]

    @iswrapper
    # ! [currenttime]
    def currentTime(self, time:int):
        super().currentTime(time)
        print("CurrentTime:", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"))
    # ! [currenttime]

    @iswrapper
    # ! [completedorder]
    def completedOrder(self, contract: Contract, order: Order,
                  orderState: OrderState):
        super().completedOrder(contract, order, orderState)
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y%m%d %H:%M:%S')
        print(timeStr, "CompletedOrder. PermId:", order.permId, "ParentPermId:", utils.longToStr(order.parentPermId), "Account:", order.account, 
              "Symbol:", contract.symbol, "SecType:", contract.secType, "Exchange:", contract.exchange, 
              "Action:", order.action, "OrderType:", order.orderType, "TotalQty:", order.totalQuantity, 
              "CashQty:", order.cashQty, "FilledQty:", order.filledQuantity, 
              "LmtPrice:", order.lmtPrice, "AuxPrice:", order.auxPrice, "Status:", orderState.status,
              "Completed time:", orderState.completedTime, "Completed Status:" + orderState.completedStatus)
    # ! [completedorder]

    @iswrapper
    # ! [completedordersend]
    def completedOrdersEnd(self):
        super().completedOrdersEnd()
        print("CompletedOrdersEnd")
    # ! [completedordersend]

def main():
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 300)
    pd.set_option('display.width', 1000)
    pd.options.display.float_format = '{:0.1f}'.format

    SetupLogger()
    logging.debug("now is %s", datetime.datetime.now())
    #logging.getLogger().setLevel(logging.INFO)
    logging.getLogger().setLevel(logging.ERROR)
    
    cmdLineParser = argparse.ArgumentParser("api tests")
    # cmdLineParser.add_option("-c", action="store_True", dest="use_cache", default = False, help = "use the cache")
    # cmdLineParser.add_option("-f", action="store", type="string", dest="file", default="", help="the input file")
    cmdLineParser.add_argument("-p", "--port", action="store", type=int,
                               dest="port", default=7497, help="The TCP port to use")
    cmdLineParser.add_argument("-C", "--global-cancel", action="store_true",
                               dest="global_cancel", default=False,
                               help="whether to trigger a globalCancel req")
    args = cmdLineParser.parse_args()
    print("Using args", args)
    logging.debug("Using args %s", args)
    # print(args)


    # enable logging when member vars are assigned
    from ibapi import utils
    Order.__setattr__ = utils.setattr_log
    Contract.__setattr__ = utils.setattr_log
    DeltaNeutralContract.__setattr__ = utils.setattr_log
    TagValue.__setattr__ = utils.setattr_log
    TimeCondition.__setattr__ = utils.setattr_log
    ExecutionCondition.__setattr__ = utils.setattr_log
    MarginCondition.__setattr__ = utils.setattr_log
    PriceCondition.__setattr__ = utils.setattr_log
    PercentChangeCondition.__setattr__ = utils.setattr_log
    VolumeCondition.__setattr__ = utils.setattr_log

    # from inspect import signature as sig
    # import code code.interact(local=dict(globals(), **locals()))
    # sys.exit(1)

    # tc = TestClient(None)
    # tc.reqMktData(1101, ContractSamples.USStockAtSmart(), "", False, None)
    # print(tc.reqId2nReq)
    # sys.exit(1)

    try:
        

        app = TestApp()
        if args.global_cancel:
            app.globalCancelOnly = True
        # ! [connect]
        #app.connect("127.0.0.1", 7496, clientId=0)
        app.connect("127.0.0.1", portTWS, clientId=0)
        # ! [connect]
        print("serverVersion:%s connectionTime:%s" % (app.serverVersion(),
                                                      app.twsConnectionTime()))


        # ! [clientrun]
        app.run()
        # ! [clientrun]
    except:
        raise
    finally:
        app.dumpTestCoverageSituation()
        app.dumpReqAnsErrSituation()


if __name__ == "__main__":
    main()
