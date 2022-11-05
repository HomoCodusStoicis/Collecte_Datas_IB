#*****************************************************************************
#  CalculPivots_Get-HLCS-quotidiens v3.py
#
#  Production fichier des HLCS entre DateDeb et DateFin pour un contrat donné
#
#       H High:   source histo bar quotidien
#       L Low :   source histo bar quotidien
#       C Close:  source histo bar 30min     (last  du Q)
#       S Settle: source histo bar quotidien (close du Q)
#
#   Sortie : 1 fichier par contrat/echeance, 
#            contenant une ligne par date entre DateInDebStr et DateInFinStr
#   les fichiers déjà existants sont sauvegardés et regénérés
#
#*****************************************************************************

portProd = 7496
portSimu = 7497
portTWS=portSimu

import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

DateInDebStr="2022-09-01"
DateInFinStr=HierStr
# DateInFinStr="2022-01-28"

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
# self.Future_NomContrat=ListeContrats[contrat_courant][0]
# self.Future_EcheanceContrat=ListeContrats[contrat_courant][1]


import argparse

import collections
import inspect
from dateutil.relativedelta import relativedelta

import logging
import time as tm
import os.path

from ibapi import wrapper
from ibapi import utils
from ibapi.client import EClient
from ibapi.utils import iswrapper

# types
from ibapi.common import * # @UnusedWildImport
from ibapi.order_condition import * # @UnusedWildImport
from ibapi.contract import * # @UnusedWildImport
from ibapi.order import * # @UnusedWildImport
from ibapi.order_state import * # @UnusedWildImport
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.ticktype import * # @UnusedWildImport
from ibapi.tag_value import TagValue

from ibapi.account_summary_tags import *

from ContractSamples import ContractSamples
#from OrderSamples import OrderSamples
#from AvailableAlgoParams import AvailableAlgoParams
#from ScannerSubscriptionSamples import ScannerSubscriptionSamples
#from FaAllocationSamples import FaAllocationSamples
#from ibapi.scanner import ScanData

#import Niveaux, Horaires
import pandas as pd

from bokeh.plotting import figure, show,  output_file, save
from math import pi
from bokeh.models.tickers import FixedTicker
from bokeh.models import DatetimeTickFormatter
from bokeh.models import DaysTicker, FuncTickFormatter, LinearAxis, Range1d
from bokeh.io import export_png

import shutil




#DateStr = DateInDebStr 
#DateStr="2021-04-22"

DateDebDt=datetime.datetime.strptime(DateInDebStr, '%Y-%m-%d')
DateFinDt=datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d')
DateCourDt = DateDebDt
DateStr = DateCourDt.strftime('%Y-%m-%d')
#DateInDebStrQuery=DateDebDt.strftime('%Y%m%d') + " 00:00:00"
DateInFinStrQuery=DateFinDt.strftime('%Y%m%d') + " 23:59:59"

#DateCourDt = DateCourDt + datetime.timedelta(days=1)
# path='Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\02 - Histo HLCS Quotidiens\\HistoHLCS_' + self.Future_NomContrat + "-Ech"+ self.Future_EcheanceContrat 
# FichierHistoHLCS = path +  '.csv'
NowStr = datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")

#FichierBackup = path + " - " + NowStr + ".csv"
# FichierOut    = FichierHistoHLCS



# Sauvegarde du fichier existant :
# try:
#     with open(FichierOut): 
#         print("Sauvegarde fichier " + FichierOut + " vers fichier " + FichierBackup + "...")
#         source=FichierOut
#         destination=FichierBackup
#         shutil.copyfile(source, destination)
# except IOError:
#         print("Fichier " + FichierOut + " inexistant : pas de sauvegarde à effectuer...")

 

 
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
    console.setLevel(logging.DEBUG)
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
        # self.Histo_ticks = pd.DataFrame(columns=['NumTrade','ts','Prix','NbLots','CumulNbLots'])
        # self.Histo_ticks.set_index("NumTrade", inplace=True)
        self.nbTicks  = 0
        self.nbTrades = 0
        self.time_dernier_tick = None

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
        
        self.tsPrec_dt =  None
        self.ts_dt =  None
        self.hlc = pd.DataFrame(columns=['Contrat','Echeance','Date', 'openJ','highJ','lowJ','closeJ','settleJ', 'volJ'])
        self.finReceptionflux1J = False
        self.finReceptionflux30m = False

        # self.FichierHistoHLCS    =   FichierHistoHLCS
        # self.TsDebutStr = TsDebutStr
        # self.TsFinStr   = TsFinStr
        # self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')
        # self.tsPrec_dt =  None
        # self.ts_dt =  None

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

        # self.contract = self.create_contract(self.Future_NomContrat, self.Future_EcheanceContrat)  # Create a contract
        self.increment_id()  # Increment the order id


        if self.globalCancelOnly:
            print("Executing GlobalCancel only")
            self.reqGlobalCancel()
        else:
 
            print("JourDeb:", DateDebDt)
            print("JourFin:", DateInFinStrQuery)
            
            #On est obligé de répcupérer l'agrégat Quotidien pour avoir les settle
            #le settle ne correspond pas exactement au close, ce sont 2 choses différentes. 
            #Le settle est un prix moyen calculé sur une plage de cotation définie qui dépend 
            #du sous jacent traité (dax, cac, Dow...)
            #On récuère l'agrégat 30min pour avoir le close (last) de la journée
            ##ts = datetime.datetime.today()

 
            self.nextValidOrderId = 10000
            self.contrat_courant=-1
            EcheanceDejaTraiteePourCeContrat = True
            #Passage au contrat suivant si pas tous déjà faits :
            while EcheanceDejaTraiteePourCeContrat == True and self.contrat_courant < len(self.ListeContrats) - 1:
                self.contrat_courant = self.contrat_courant + 1
                self.Future_NomContrat=self.ListeContrats[self.contrat_courant][0]
                self.Future_EcheanceContrat=self.ListeContrats[self.contrat_courant][1]
                print("----------------------")
                print("Start - Contrat suivant : " + self.Future_NomContrat + "- Ech"+ self.Future_EcheanceContrat)
                print("----------------------")
                self.contract = self.create_contract(self.Future_NomContrat, self.Future_EcheanceContrat)  # Create a contract
     
                
                repertoire = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\02 - Histo HLCS Quotidiens'
                repertoireB= repertoire + '\\Backup'
                path = repertoire + '\\HistoHLCS_' + self.Future_NomContrat + "-Ech"+ self.Future_EcheanceContrat
                pathB = repertoireB + '\\HistoHLCS_' + self.Future_NomContrat + "-Ech"+ self.Future_EcheanceContrat


                #FichierHistoHLCS = path +  '.csv'
    
                if not os.path.exists(repertoire):
                    os.makedirs(repertoire)
    
                self.DateStr    = DateStr
                self.DateDebDt  = DateDebDt
                self.DateFinDt  = DateFinDt
                
                # self.tsPrec_dt =  None
                # self.ts_dt =  None
    
                tsStr = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H-%M-%S")
                self.DateCourDt =  self.DateDebDt
                self.FichierHistoHLCS    =   path + '.csv'
                self.FichierHistoHLCSBackup = pathB + '-' + tsStr + '.bac'
                # self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
                # self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
                # self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')
     
                # self.nbTicks = 0
                # self.nbTrades = 0
                # self.hlc = self.hlc.drop(self.hlc.index)
                #print('===========4 ' + self.Future_NomContrat + ' - ' + self.Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                #print(self.Histo_ticks)
    
                #Si fichier déjà existant, on passe à la journée suivante :
                try:
                    with open(self.FichierHistoHLCS): 
                        print('Echéance contrat déjà traitée:', self.Future_NomContrat + "- Ech"+ self.Future_EcheanceContrat)
                        print('Sauvagerde du fichier existant...'+self.FichierHistoHLCS + ' en ' + self.FichierHistoHLCSBackup)
                        shutil.move(self.FichierHistoHLCS, self.FichierHistoHLCSBackup)
                        EcheanceDejaTraiteePourCeContrat = False
                except IOError:
                        EcheanceDejaTraiteePourCeContrat = False
    

    
            if EcheanceDejaTraiteePourCeContrat == False:
                # self.nbTicks = 0
                # self.nbTrades = 0
                self.hlc = self.hlc.drop(self.hlc.index)
                # print('===========3 ' + self.Future_NomContrat + ' - ' + self.Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                #print(self.Histo_ticks)
                
                ts = datetime.datetime.now()
    
                deltaDays = (DateFinDt - DateDebDt).days
                deltaDaysStr = str(deltaDays+1) + ' D'
                print('Demande historique sur ', deltaDaysStr, "jusqu'au", DateInFinStrQuery)
                
                print(ts,"Appel requete reqHistoricalData DAY...",self.nextValidOrderId)
                #queryTime = (datetime.datetime.today()).strftime("%Y%m%d %H:%M:%S")
                self.reqHistoricalData(self.nextValidOrderId, self.contract, DateInFinStrQuery,
                                  deltaDaysStr, "1 day", "TRADES", 0, 1, False, [])            


                # tm.sleep(3)
                
                self.nextValidOrderId = self.nextValidOrderId + 1
                print(ts,"Appel requete reqHistoricalData 30 mins...", self.nextValidOrderId)
                self.reqHistoricalData(self.nextValidOrderId, self.contract, DateInFinStrQuery,
                                  deltaDaysStr, "30 mins", "TRADES", 0, 1, False, [])


            else:
                print("fin reception flux de toutes les échéances demandées pour tous les contrats...")
                # self.done = True
                




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
        print("Executing STOP")
        # self.orderOperations_cancel()
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
        # self.tickByTickOperations_cancel()
        # print("Executing cancels ... finished")
        # print("==============================================================")
        # print("    Historique des ordres ")
        # print("==============================================================")
        # print(self.Histo_Mes_Ordres)
        # print("==============================================================")

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
            contract.exchange = "GLOBEX"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = EcheanceContrat
            contract.multiplier = "20"

        else:
            print("Contrat non implémenté:", NomContrat)

        return contract


            
    # ! [orderstatus]


         

    def get_points_pivots(self, option:str, bar: BarData):
        
        if option == 'getHisto':
            
            
 
            print("JourDeb:", DateDebDt)
            print("JourFin:", DateInFinStrQuery)
            
            #On est obligé de répcupérer l'agrégat Quotidien pour avoir les settle
            #le settle ne correspond pas exactement au close, ce sont 2 choses différentes. 
            #Le settle est un prix moyen calculé sur une plage de cotation définie qui dépend 
            #du sous jacent traité (dax, cac, Dow...)
            #On récuère l'agrégat 30min pour avoir le close (last) de la journée
            ##ts = datetime.datetime.today()
            ts = DateFinDt


            deltaDays = (DateFinDt - DateDebDt).days
            deltaDaysStr = str(deltaDays+1) + ' D'
            print(deltaDaysStr)
            

            print(ts,"get_points_pivots - Option getHisto => appel requete reqHistoricalData DAY...")
            #queryTime = (datetime.datetime.today()).strftime("%Y%m%d %H:%M:%S")
            self.reqHistoricalData(10900, self.contract, DateInFinStrQuery,
                              deltaDaysStr, "1 day", "TRADES", 0, 1, False, [])
            tm.sleep(3)
            print(ts,"get_points_pivots - Option getHisto => appel requete reqHistoricalData 30 mins...")
            self.reqHistoricalData(10901, self.contract, DateInFinStrQuery,
                              deltaDaysStr, "30 mins", "TRADES", 0, 1, False, [])


    @iswrapper
    # ! [historicaldata]
    def historicalData(self, reqId:int, bar: BarData):

        # print( "reqId:", reqId, "bar:", bar)
        
        #Histo par jour:
        if reqId % 2 == 0:
            DateDt=datetime.datetime.strptime(bar.date, '%Y%m%d')
            
            self.hlc = self.hlc.append({'Contrat':self.Future_NomContrat ,'Echeance':self.Future_EcheanceContrat ,
                                        'Date':DateDt,'openJ': bar.open, 'highJ':bar.high,'lowJ':bar.low, 
                                        'settleJ':bar.close, 'volJ':bar.volume}, ignore_index=True)
            
       #Histo par 30 min:
        if reqId % 2 == 1:
            DateDt=datetime.datetime.strptime(bar.date[:8], '%Y%m%d')
            F=(self.hlc['Date']==DateDt)
            self.hlc.loc[F,'closeJ']=bar.close
            #print( "reqId:", reqId, "bar:", bar)

            
        
        if self.debutReceptionFlux5min:
            ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
            # print(ts, "reqId:", reqId, "bar:", bar)
            self.debutReceptionFlux5min = False      
        
    # ! [historicaldata]

    @iswrapper
    # ! [historicaldataend]
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print("Fin réception flux - HistoricalDataEnd - ReqId:", reqId, "from", start, "to", end)
        #self.get_points_pivots("Calculer", None)
        #Histo par jour:
        if reqId % 2 == 0:
            self.finReceptionflux1J = True
        #Histo par 30 min:
        if reqId % 2 == 1:
            self.finReceptionflux30m = True

        if self.finReceptionflux1J & self.finReceptionflux30m:

            self.finReceptionflux1J = False
            self.finReceptionflux30m = False
            
            self.hlc.to_csv(self.FichierHistoHLCS,sep=';',decimal='.',float_format='%.1f', index=False)


            # Passage au contrat suivant :
                
            # self.nextValidOrderId = self.nextValidOrderId + 1
            # self.contrat_courant=-1
            EcheanceDejaTraiteePourCeContrat = True
            #Passage au contrat suivant si pas tous déjà faits :
            while EcheanceDejaTraiteePourCeContrat == True and self.contrat_courant < len(self.ListeContrats) - 1:
                self.contrat_courant = self.contrat_courant + 1
                self.Future_NomContrat=self.ListeContrats[self.contrat_courant][0]
                self.Future_EcheanceContrat=self.ListeContrats[self.contrat_courant][1]
                print("----------------------")
                print("historicalDataEnd - Contrat suivant : " + self.Future_NomContrat + "- Ech"+ self.Future_EcheanceContrat)
                print("----------------------")
                self.contract = self.create_contract(self.Future_NomContrat, self.Future_EcheanceContrat)  # Create a contract
     
                
                repertoire = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\02 - Histo HLCS Quotidiens'
                repertoireB= repertoire + '\\Backup'
                path = repertoire + '\\HistoHLCS_' + self.Future_NomContrat + "-Ech"+ self.Future_EcheanceContrat
                pathB = repertoireB + '\\HistoHLCS_' + self.Future_NomContrat + "-Ech"+ self.Future_EcheanceContrat


                #FichierHistoHLCS = path +  '.csv'
    
                # if not os.path.exists(repertoire):
                #     os.makedirs(repertoire)
    
                self.DateStr    = DateStr
                self.DateDebDt  = DateDebDt
                self.DateFinDt  = DateFinDt
                
                # self.tsPrec_dt =  None
                # self.ts_dt =  None
                tsStr = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H-%M-%S")
                self.DateCourDt =  self.DateDebDt
                self.FichierHistoHLCS    =   path + '.csv'
                self.FichierHistoHLCSBackup = pathB + '-' + tsStr + '.bac'
                # self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
                # self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
                # self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')
     
                # self.nbTicks = 0
                # self.nbTrades = 0
                # self.hlc = self.hlc.drop(self.hlc.index)
                #print('===========4 ' + self.Future_NomContrat + ' - ' + self.Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                #print(self.Histo_ticks)
    
                #Si fichier déjà existant, on passe à la journée suivante :
                try:
                    with open(self.FichierHistoHLCS): 
                        print('Echéance contrat déjà traitée:', self.Future_NomContrat + "- Ech"+ self.Future_EcheanceContrat)
                        print('Sauvagerde du fichier existant...'+self.FichierHistoHLCS + ' en ' + self.FichierHistoHLCSBackup)
                        shutil.move(self.FichierHistoHLCS, self.FichierHistoHLCSBackup)
                        EcheanceDejaTraiteePourCeContrat = False
                except IOError:
                        EcheanceDejaTraiteePourCeContrat = False
    

    
            if EcheanceDejaTraiteePourCeContrat == False:
                # self.nbTicks = 0
                # self.nbTrades = 0
                self.hlc = self.hlc.drop(self.hlc.index)
                # print('===========3 ' + self.Future_NomContrat + ' - ' + self.Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)
                #print(self.Histo_ticks)
                
                ts = datetime.datetime.now()
    
                deltaDays = (DateFinDt - DateDebDt).days
                deltaDaysStr = str(deltaDays+1) + ' D'
                print('Demande historique sur ', deltaDaysStr, "jusqu'au", DateInFinStrQuery)
                
                #queryTime = (datetime.datetime.today()).strftime("%Y%m%d %H:%M:%S")
                self.nextValidOrderId = self.nextValidOrderId + 1
                print(ts,"Appel requete reqHistoricalData DAY...",self.nextValidOrderId)
                self.reqHistoricalData(self.nextValidOrderId, self.contract, DateInFinStrQuery,
                                  deltaDaysStr, "1 day", "TRADES", 0, 1, False, [])            


                # tm.sleep(3)
                
                self.nextValidOrderId = self.nextValidOrderId + 1
                print(ts,"Appel requete reqHistoricalData 30 mins...", self.nextValidOrderId)
                self.reqHistoricalData(self.nextValidOrderId, self.contract, DateInFinStrQuery,
                                  deltaDaysStr, "30 mins", "TRADES", 0, 1, False, [])
                
                
            else:
    
                # print(self.hlc)
                print("fin reception flux de toutes les échéances demandées pour tous les contrats...")
                self.stop()

        
    # ! [historicaldataend]




    @iswrapper
    # ! [currenttime]
    def currentTime(self, time:int):
        super().currentTime(time)
        print("CurrentTime:", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"))
    # ! [currenttime]



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
