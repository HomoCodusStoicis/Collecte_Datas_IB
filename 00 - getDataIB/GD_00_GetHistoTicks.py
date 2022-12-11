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
        self.nbTicksPrec = 0

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
            contract.exchange = "CBOT"
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

    @iswrapper
    # ! [historicaltickslast] 88888888888
    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast,
                            done: bool):
        tsNow = datetime.datetime.today()
        print(datetime.datetime.today(), 'Réponse historicalTicksLast,  reqId:',reqId)
        
       
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
            print(datetime.datetime.today(), 'Reponse historicalTicksLast, reqId:',reqId, "- done=", done, "  - Size ticks rendus:",len(ticks),
                  "- 1er / dernier tick rendu:", datetime.datetime.fromtimestamp(ticks[0].time),
                  "-", datetime.datetime.fromtimestamp(ticks[-1].time))
        else:
            print(datetime.datetime.today(), 'Reponse historicalTicksLast, reqId:',reqId, "- done=", done, "  - Size ticks rendus:",len(ticks))

        ts_next_dt = datetime.datetime.strptime(self.ts_next, '%Y%m%d %H:%M:%S') 
        

        if done and len(ticks) == 0 and not TickVideConfirmed and self.time_dernier_tick < self.ts_fin_du_jour :
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
                print(datetime.datetime.today(), " Flux complet pour jour ", self.DateStr, " - Dernier tick du jour à ",self.time_dernier_tick)
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
                            print(datetime.datetime.today(), ' Journée déjà traitée:', self.DateStr)
                            JourneeDejaTraiteePourCeContrat = True
                    except IOError:
                            JourneeDejaTraiteePourCeContrat = False
    
                if JourneeDejaTraiteePourCeContrat == False:
                    self.nbTicks = 0
                    self.nbTrades = 0
                    self.Histo_ticks = self.Histo_ticks.drop(self.Histo_ticks.index)
                    print("\n", datetime.datetime.today(), ' ===========1 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                    #print(self.Histo_ticks)
                    print(datetime.datetime.today(), ' appel   reqHistoricalTicks, reqId:',reqId+1, self.TsDebutStr)
                    self.reqHistoricalTicks(reqId+1, self.contract, self.TsDebutStr, "", NbTicksDemandes, "TRADES", 0, True, []);
                else:
                    print(datetime.datetime.today(), " fin reception flux de toutes les journées demandées pour ce contrat...")
    
                    
                    #Passage au contrat suivant si pas tous déjà faits :
                    JourneeDejaTraiteePourCeContrat = True
                    while JourneeDejaTraiteePourCeContrat == True and self.contrat_courant < len(self.ListeContrats) - 1:
                        self.contrat_courant = self.contrat_courant + 1
                        Future_NomContrat=ListeContrats[self.contrat_courant][0]
                        Future_EcheanceContrat=ListeContrats[self.contrat_courant][1]
                        print(datetime.datetime.today(), " ----------------------")
                        print(datetime.datetime.today(), " Contrat suivant : " + Future_NomContrat + "- Ech"+ Future_EcheanceContrat)
                        print(datetime.datetime.today(), " ----------------------")
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
                                print(datetime.datetime.today(), " Journée déjà traitée:", self.DateStr)
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
                                    print(datetime.datetime.today(), ' Journée déjà traitée:', self.DateStr)
                                    JourneeDejaTraiteePourCeContrat = True
                            except IOError:
                                    JourneeDejaTraiteePourCeContrat = False
    
                        if JourneeDejaTraiteePourCeContrat == False:
                            self.nbTicks = 0
                            self.nbTrades = 0
                            self.Histo_ticks = self.Histo_ticks.drop(self.Histo_ticks.index)
                            print(datetime.datetime.today(), ' ===========2 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                            #print(self.Histo_ticks)
                            print(datetime.datetime.today(), ' appel   reqHistoricalTicks, reqId:',reqId+1, self.TsDebutStr)
                            self.reqHistoricalTicks(reqId+1, self.contract, self.TsDebutStr, "", NbTicksDemandes, "TRADES", 0, True, []);
                        else:
                            print(datetime.datetime.today(), " fin reception flux de toutes les journées demandées pour ce contrat...")
    
                            
                        
                     
                    print(datetime.datetime.today(), " fin traitement de tous les contrats..")
                    #self.stop()
                
            


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
