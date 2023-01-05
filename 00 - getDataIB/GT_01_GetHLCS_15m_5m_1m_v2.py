#*****************************************************************************
#  GT_01_GetHLCS_15m_5m_1m_v2.py
#
#  Production fichier des HLCS entre DateDeb et DateFin pour un contrat donné
#  Périodes 15min, 5min, 1min
#  
#   Sortie : 1 fichier par contrat/echeance/jour
#   les dates pour lesquelles les fichiers existent sont ignorées
#
#*****************************************************************************

# Mock_Api_Ib = True
# Mock_TS_Cour = "20211223 10:00:00"

import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

DateInDebStr="2023-01-03 00:00:00"

DateInFinStr = HierStr + " 23:59:59"
#DateInFinStr = "2022-03-19 03:59:59"

portProd = 7496
portSimu = 7497
portTWS=portSimu
import datetime
# DateStr = datetime.datetime.today().strftime("%Y-%m-%d")
# DateStr = "2021-12-23"

repOut = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\01 - Histo Bars Minutes'

ListeContratsIn = [["NASDAQ-mini",["202303","202306"]],
                   ["DOW-mini"   ,["202303","202306"]],
                   ["DAX-mini"   ,["202303","202306"]]
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
import numpy
import collections
import inspect
import shutil

import logging
import time as tm
import os.path

import threading

from scipy.stats.mstats import gmean
import talib as tb


# NowStr = datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")
DateInDebDt=datetime.datetime.strptime(DateInDebStr, '%Y-%m-%d %H:%M:%S')
DateInFinDt=datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d %H:%M:%S')
DateDebStr    = DateInDebDt.strftime('%Y-%m-%d')
DateFinStr    = DateInFinDt.strftime('%Y-%m-%d')
DateCourDt = DateInDebDt

# Librairies de l'API IB
from ibapi import wrapper
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi import utils
from ibapi.client import EClient
from ibapi.utils import iswrapper
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
# from OrderSamples import OrderSamples

# Librairies persos
# import Niveaux, Horaires

import pandas as pd

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 300)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:0.1f}'.format


# FichierNiveauxDuJour   = 'Y:\TRAVAIL\Mes documents\Bourse\Python\Robot IB\Robot IB\\NiveauxDuJour\\NiveauxCalcules-' + DateStr + '.csv'
# FichierHistoOrdresJour = 'Y:\TRAVAIL\Mes documents\Bourse\Python\Robot IB\Robot IB\Suivi exécutions\\Q' + DateStr + '-HistoOrdres.csv'
# NomFichierNiveauxEnrichi   = 'Y:\TRAVAIL\Mes documents\Bourse\Python\Robot IB\Robot IB\\Suivi exécutions\\Q' + DateStr + '-NiveauxEnrichis.csv'

# try:
#     with open(FichierNiveauxDuJour): 
#         print("GetHisto = False")
#         GetHisto = False
# except IOError:
#         print("GetHisto = True")
#         GetHisto = True
 
GetHisto = False
 
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




# ! [socket_init]
class IBApi(EWrapper, EClient):

    def __init__(self):
        print(datetime.datetime.today(),"__init__")
        #TestWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
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
        self.echeance=None
        self.periode=None
        self.DateDebDt = DateInDebDt
        self.DateCourDt = DateInDebDt
        self.DateFinDt = DateInFinDt
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
        self.Histo_Mes_Ordres = pd.DataFrame(columns=['ID','Niveau','Bracket','Sens','Prix','Nb','TickValidite','Etat','NbTrt','NbRest','PrixExec', 'Bilan', 'TsExec', 'Comment','IdParent'])
        self.Histo_Mes_Ordres.set_index("ID", inplace=True)
        self.debutReceptionFlux5min = True
        self.NbPeriodesDebitOK = 0
 
        # Paramètres ajustables
        self.stop_loss_S1   = 15
        self.stop_profit_S1 = 3
        self.nb_ticks_minimum_avant_reactivation_niveau = 1000
        self.duree_minimum_avant_reactivation_niveau    = 120
        self.seuilFusionNiveaux = 10 # ecart de points sous lesquel on fusionnera 2 niveaux trop proches
        self.ParamDureePeriodecalculDebit = 30   # Nb ticks à prendre en compte pour réévaluation si période est risquée du point de vue du débit
        self.ParamDebitTicksMax = 7   #débit max en nbTicks / sec
        self.ParamNbPeriodeDebitOK = 10  # Après une periode de debit trop elevé, combien faut-il avoir de periodes calmes pour réctiver le trading ?
        self.ParamDistanceTriggers = 10 # distance des triggers par rapport à leur niveau de référence
        self.DureeVieOrdreParent_Ticks = 1000 #si l'ordre positionné il y a 1000 ticks n'a toujours pas été activé, c'est probablement qu'on est en train de ranger juste en dessous, c'est dangereux, on risque le breakOut
        self.FrequenceVerificationSituationRange = 50 # verif tous les 50 ticks    
        
        print(datetime.datetime.today(),"__init__", self.started)


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
        print(datetime.datetime.today(),"start", self.started)

        if self.started:
            return

        self.started = True

        num_contrat_courant=0
        self.fin_flux = 0
        self.ListeContrats   = ListeContrats
        self.contrat_courant=-1
        # JourneeDejaTraiteePourCeContrat = True

        ##############################################
        # Nouveau CODE
        ##############################################
        
        print("")
        print("===================================================")
        print("===================================================")
        print("Start - Traitement des flux en 15 min...")
        print("===================================================")
        print("===================================================")

        reqId = 18000
        self.contrat_courant=0
        
        Future_NomContrat=self.ListeContrats[self.contrat_courant][0]
        Future_EcheanceContrat=self.ListeContrats[self.contrat_courant][1]
        print("")
        print("----------------------")
        print("Start - Contrat suivant : " + Future_NomContrat + "- Ech"+ Future_EcheanceContrat)
        print("----------------------")
        
        self.Determine_Et_Appelle_Requete_Suivante("15min")
        
        #Trt_Une_Time_Unit("5min")
        
        #Trt_Une_Time_Unit("1min")
        ##############################################

       
        print(datetime.datetime.today(),"Executing requests ... finished")

    def increment_id(self):
        """ Increments the request id"""

        self.reqID = self.reqID + 1

    def nextOrderId(self):
        self.nextValidOrderId += 1
        oid = self.nextValidOrderId
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
        
        if NomContrat in ["DXM", "DAX-mini"]:
            contract.symbol = "DAX"
            contract.secType = "FUT"
            contract.exchange = "EUREX"
            contract.currency = "EUR"
            contract.lastTradeDateOrContractMonth = EcheanceContrat  #202106
            contract.multiplier = "5"

        elif NomContrat in ["YM", "DOW-mini"]:
            contract.symbol = "YM"
            contract.secType = "FUT"
            contract.exchange = "CBOT"
            contract.currency = "USD"
            contract.lastTradeDateOrContractMonth = EcheanceContrat
            contract.multiplier = "5"

        elif NomContrat in ["NQ", "NASDAQ-mini"]:
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
    # ! [historicaldata]
    def historicalData(self, reqId:int, bar: BarData):
        #print(datetime.datetime.today(), threading.current_thread().name , "historicalData - ReqId:", reqId, ' Bar:', bar.date)

        isPeriodeCourante15min = False
        isPeriodeCourante5min  = False
        isPeriodeCourante1min  = False

        #on ne stocke la bougie que si celle-ci est cloturée : si la période est terminée
        updateToDo= True
        F=(bot.cpt['reqId'] == reqId)
        PeriodeReq = bot.cpt.loc[F,'Periode'].values[0]
        if PeriodeReq != '1D' :
            ts_cour = datetime.datetime.today()
            delta_time = ts_cour - datetime.datetime.strptime(bar.date, '%Y%m%d %H:%M:%S')
            isPeriodeCourante15min = delta_time < datetime.timedelta(minutes=15)
            isPeriodeCourante5min  = delta_time < datetime.timedelta(minutes=5)
            isPeriodeCourante1min  = delta_time < datetime.timedelta(minutes=1)
        
        if PeriodeReq == '15min' and isPeriodeCourante15min:
            updateToDo=False
        if PeriodeReq == '5min' and isPeriodeCourante5min:
            updateToDo=False
        if PeriodeReq == '1min' and isPeriodeCourante1min:
            updateToDo=False

        if updateToDo:  
            bot.on_bar_update_histo(reqId, bar)
            
    # ! [historicaldata]

    @iswrapper
    # ! [historicaldataend]
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        super().historicalDataEnd(reqId, start, end)
        print(datetime.datetime.today(),threading.current_thread().name , "Fin réception flux - HistoricalDataEnd - ReqId:", reqId, "from", start, "to", end)
        
        bot.on_historicalDataEnd(reqId)
   
    # ! [historicaldataend]

   

    @iswrapper
    # ! [currenttime]
    def currentTime(self, time:int):
        super().currentTime(time)
        print("CurrentTime:", datetime.datetime.fromtimestamp(time).strftime("%Y%m%d %H:%M:%S"))
    # ! [currenttime]



class Bot():


    ib = None    
    
    def __init__(self):    
    
        SetupLogger()
        logging.error("now is %s", datetime.datetime.now())
        #logging.getLogger().setLevel(logging.INFO)
        logging.getLogger().setLevel(logging.ERROR)
        
        cmdLineParser = argparse.ArgumentParser("api tests")
        cmdLineParser.add_argument("-p", "--port", action="store", type=int,
                                    dest="port", default=portTWS, help="The TCP port to use")
        cmdLineParser.add_argument("-C", "--global-cancel", action="store_true",
                                    dest="global_cancel", default=False,
                                    help="whether to trigger a globalCancel req")
        args = cmdLineParser.parse_args()
        print(datetime.datetime.today(),"Using args", args)
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
    
        #Dataframes pour stocker historique data, mis à jour dans on_bar_update :
        self.Histo_ohlc_day = pd.DataFrame(columns=['Contrat','Date','open','high','low','close','Volume','EMA20','EMA50','EMA05','RSI14','xopen','xclose','xlow','xhigh','xcouleur'])
        #self.Histo_ohlc_day.set_index(["Contrat","Date"], inplace=True)
        self.Histo_ohlc_15min = pd.DataFrame(columns=['Contrat','Echeance','Ts','open','high','low','close','Volume','EMA20','EMA50','EMA05','RSI14','xopen','xclose','xlow','xhigh','xcouleur'])
        #self.Histo_ohlc_15min.set_index(["Contrat","Ts"], inplace=True)
        self.Histo_ohlc_5min = pd.DataFrame(columns=['Contrat','Echeance','Ts','open','high','low','close','Volume','EMA20','EMA50','EMA05','RSI14','xopen','xclose','xlow','xhigh','xcouleur'])
        #self.Histo_ohlc_5min.set_index(["Contrat","Ts"], inplace=True)
        self.Histo_ohlc_1min = pd.DataFrame(columns=['Contrat','Echeance','Ts','open','high','low','close','Volume','EMA20','EMA50','EMA05','RSI14','xopen','xclose','xlow','xhigh','xcouleur'])
        #self.Histo_ohlc_1min.set_index(["Contrat","Ts"], inplace=True)
        self.i_day = 0
        self.i_15min=0
        self.i_5min=0
        self.i_1min=0
        self.i_day_2 = 0
        
        self.contract=None
        self.echeance=None
        self.periode=None
        self.contrat_courant = 0
        self.DateDebDt = DateInDebDt
        self.DateCourDt = DateInDebDt + datetime.timedelta(days=-1)
        self.DateFinDt = DateInFinDt
    
    
        try:

               
            self.ib = IBApi()
    
            self.ib.connect("127.0.0.1", portTWS, clientId=0)
     
            logging.error("serverVersion:%s connectionTime:%s" % (self.ib.serverVersion(),
                                                          self.ib.twsConnectionTime()))

                
            logging.error("Avant creation thread...")
            ib_thread = threading.Thread(target=self.run_loop, daemon=True)
            logging.error("Après creation thread...")
            ib_thread.start()
            logging.error("Après start thread...")
            
            # Start algo :
            logging.error("Dans start...")

            
        except:
            raise

            
        logging.error("Après lancement thread ib...")
        
   
        
    def run_loop(self):
        print(datetime.datetime.today(),"run_loop")
        self.ib.run()        

    def Determine_Et_Appelle_Requete_Suivante(self,TimeUnit):
            
        # TimeUnit : param d'entrée, valeur in {'15min','5min','1min'}
        
        #Recherche Journée pas déjà traitée, passage au lendemain:
        JourneeDejaTraiteePourCeContrat = True
        while JourneeDejaTraiteePourCeContrat == True and self.DateCourDt < self.DateFinDt:
                
            self.DateCourDt = self.DateCourDt + datetime.timedelta(days=1)
            self.DateStr = self.DateCourDt.strftime('%Y-%m-%d')
            Future_NomContrat=ListeContrats[self.contrat_courant][0]
            Future_EcheanceContrat=ListeContrats[self.contrat_courant][1]
            self.contract = self.ib.create_contract(Future_NomContrat, Future_EcheanceContrat)  # Create a contract
            
            repertoire = repOut     + '\\HistoBars_'            + Future_NomContrat + "-Ech"+ Future_EcheanceContrat
            path       = repertoire + '\\HistoBars-' + TimeUnit + Future_NomContrat + "-Ech"+ Future_EcheanceContrat + "_Q"
            ficOut     = path + self.DateStr + '.csv'
            
            #self.FichierHistoTicksJour    =   path + self.DateStr + '.csv'
            self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
            self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
            self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')

            #Si fichier déjà existant, on passe à la journée suivante :
            try:
                with open(ficOut): 
                    print(datetime.datetime.today(), ' Journée déjà traitée:', self.DateStr)
                    JourneeDejaTraiteePourCeContrat = True
            except IOError:
                    JourneeDejaTraiteePourCeContrat = False

        if JourneeDejaTraiteePourCeContrat == False:
            print('===========3 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)

            d = self.DateStr
            DateFinStrQuery = d[0:4] + d[5:7] + d[8:10] + '-23:59:59'
            DateCourDt = self.DateCourDt
            DateCourD = DateCourDt.date()
            
            dic = {'15min' : '15 mins', '5min':'5 mins', '1min': '1 min'}
            BarSize = dic[TimeUnit]
            self.ib.increment_id()
            print(datetime.datetime.today(), "Appel requete reqHistoricalData " + BarSize + "..." + Future_NomContrat + ' - ' + Future_EcheanceContrat + 
                    ' - Jour :', self.DateStr + " - IdReq = " + str(self.ib.reqID))
        
            self.contrat  = Future_NomContrat
            self.echeance = Future_EcheanceContrat
            self.periode  = TimeUnit 
            self.ib.reqHistoricalData(self.ib.reqID, self.contract, DateFinStrQuery,'1 D', BarSize, "TRADES", 0, 1, False, [])

        else:
            print(datetime.datetime.today(), " fin reception flux de toutes les journées demandées pour ce contrat pour l'unité de temps " + TimeUnit + " ...")
                    
                    
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

                repertoire = repOut     + '\\HistoBars_'            + Future_NomContrat + "-Ech"+ Future_EcheanceContrat
                path       = repertoire + '\\HistoBars-' + TimeUnit + Future_NomContrat + "-Ech"+ Future_EcheanceContrat + "_Q"

                if not os.path.exists(repertoire):
                    os.makedirs(repertoire)

                self.DateStr    = DateStr
                self.DateDebDt  = DateDebDt
                self.DateFinDt  = DateFinDt
                
                self.tsPrec_dt =  None
                self.ts_dt =  None

                self.DateCourDt =  self.DateDebDt
                ficOut = path + self.DateStr + '.csv'

                #self.FichierHistoTicksJour    =   path + self.DateStr + '.csv'
                self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:01"
                self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 22:05:00"
                self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')

                print('===========4 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)

                #Si fichier déjà existant, on passe à la journée suivante :
                try:
                    with open(ficOut): 
                        print('Journée déjà traitée:', self.DateStr)
                        JourneeDejaTraiteePourCeContrat = True
                except IOError:
                        JourneeDejaTraiteePourCeContrat = False

                #Recherche Journée pas déjà traitée, passage au lendemain:
                while JourneeDejaTraiteePourCeContrat == True and self.DateCourDt < self.DateFinDt:
                        
                    self.DateCourDt = self.DateCourDt + datetime.timedelta(days=1)
                    self.DateStr = self.DateCourDt.strftime('%Y-%m-%d')
                    self.FichierHistoTicksJour    =   path + self.DateStr + '.csv'
                    self.TsDebutStr = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 00:00:00"
                    self.TsFinStr   = self.DateStr[0:4] + self.DateStr[5:7] + self.DateStr[8:10] + " 23:59:59"
                    self.ts_fin_du_jour = datetime.datetime.strptime(self.TsFinStr,  '%Y%m%d  %H:%M:%S')

                    #Si fichier déjà existant, on passe à la journée suivante :
                    try:
                        with open(self.FichierHistoTicksJour): 
                            print('Journée déjà traitée:', self.DateStr)
                            JourneeDejaTraiteePourCeContrat = True
                    except IOError:
                        JourneeDejaTraiteePourCeContrat = False

            if JourneeDejaTraiteePourCeContrat == False:

                print('===========3 ' + Future_NomContrat + ' - ' + Future_EcheanceContrat + ' - Flux journée suivante :', self.DateStr)

                d = self.DateStr
                DateFinStrQuery = d[0:4] + d[5:7] + d[8:10] + '-23:59:59'
                DateCourDt = self.DateCourDt
                DateCourD = DateCourDt.date()
                
                dic = {'15min' : '15 mins', '5min':'5 mins', '1min': '1 min'}
                BarSize = dic[TimeUnit]
                #print(datetime.datetime.today(),"BarSize:",BarSize)
                self.ib.increment_id()
                print(datetime.datetime.today(), "Appel requete reqHistoricalData " + BarSize + "..." + Future_NomContrat + ' - ' + Future_EcheanceContrat + 
                        ' - Jour :', self.DateStr + " - IdReq = " + str(self.ib.reqID))
            
                self.contrat  = Future_NomContrat
                self.echeance = Future_EcheanceContrat
                self.periode  = TimeUnit 
                self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,'1 D', BarSize, "TRADES", 0, 1, False, [])

            else:
                print("fin reception flux de toutes les journées demandées pour tous les contrats pour l'unité de temps " + TimeUnit + "...")
                
                if TimeUnit == '15min':
                    print("")
                    print("===================================================")
                    print("===================================================")
                    print("Start - Traitement des flux en 5 min...")
                    print("===================================================")
                    print("===================================================")
                    self.Determine_Et_Appelle_Requete_Suivante('5min')
                elif TimeUnit == '5min':
                    print("")
                    print("===================================================")
                    print("===================================================")
                    print("Start - Traitement des flux en 1 min...")
                    print("===================================================")
                    print("===================================================")                   
                    self.Determine_Et_Appelle_Requete_Suivante('1min')
                else:
                    print("fin reception flux de toutes les journées demandées pour tous les contrats pour TOUTES les unités de temps...")

    def on_bar_update_histo(self, reqId, bar):

        #print(datetime.datetime.today(), threading.current_thread().name , "on_bar_update_histo - ReqId:", reqId, ' Bar:', bar.date)
        
        Contrat = self.contrat
        Periode = self.periode
        Echeance = self.echeance

           
        #  15min : 
        if Periode == '15min':
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_15min=self.i_15min+1
            i=self.i_15min
            self.Histo_ohlc_15min.loc[i] = [Contrat, Echeance, date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]
            
        #  5min :
        if Periode == '5min':
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_5min=self.i_5min+1
            i=self.i_5min
            self.Histo_ohlc_5min.loc[i] = [Contrat, Echeance, date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]

        #  1min :
        if Periode == '1min':
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_1min=self.i_1min+1
            i=self.i_1min
            self.Histo_ohlc_1min.loc[i] = [Contrat, Echeance, date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]

    def  ecrire_fichier(self, reqId, DateD):

        ts = datetime.datetime.today()         

        Contrat = self.contrat
        Periode = self.periode
        Echeance = self.echeance
        print(ts, " ecrire_fichier, reqId:", reqId, DateD, Contrat, Periode)

        DateStr = DateD.strftime('%Y-%m-%d')
        dos = '\\HistoBars_' + Contrat + '-Ech' + Echeance 
        ficOut = repOut + dos + '\\HistoBars-' + Periode + '-' + Contrat + '-Ech' + Echeance + '-Q' + DateStr + '.csv'

        if not os.path.exists(repOut + dos):
            os.makedirs(repOut + dos)

        # ficOutBackup = repertoireOut + '\\Backup\\Histo-' + iPeriode + '-' + iContrat + '-' + DateStr + '-' + tsStr + '.bac'
        # Test fichier resultat déjà existant :
        try:
            with open(ficOut): 
                print(datetime.datetime.today() ,"Fichier déjà existant : " +  ficOut )
                # print('Backup du fichier '+ ficOut + ' en ' + ficOutBackup)
                # shutil.move(ficOut, ficOutBackup)
                FichierATraiter = False
        except IOError:
                FichierATraiter = True
                
        if FichierATraiter:
            print(datetime.datetime.today() ,"Ecriture fichier " + ficOut)

            if Periode == '1min':
                F_c = (self.Histo_ohlc_1min['Contrat']   == Contrat)
                F_e = (self.Histo_ohlc_1min['Echeance']   == Echeance)
                F_d = (self.Histo_ohlc_1min['Ts'].apply(lambda x: x.date()) == DateD)
                df  = self.Histo_ohlc_1min.loc[F_c & F_e & F_d]
            if Periode == '5min':
                F_c = (self.Histo_ohlc_5min['Contrat']   == Contrat)
                F_e = (self.Histo_ohlc_5min['Echeance']   == Echeance)
                F_d = (self.Histo_ohlc_5min['Ts'].apply(lambda x: x.date()) == DateD)
                df  = self.Histo_ohlc_5min.loc[F_c & F_e & F_d]
            if Periode == '15min':
                F_c = (self.Histo_ohlc_15min['Contrat']   == Contrat)
                F_e = (self.Histo_ohlc_15min['Echeance']   == Echeance)
                F_d = (self.Histo_ohlc_15min['Ts'].apply(lambda x: x.date()) == DateD)
                df  = self.Histo_ohlc_15min.loc[F_c & F_e & F_d]
                
            df.to_csv(ficOut,sep=';',decimal='.',float_format='%.1f', index=False)

    def  on_historicalDataEnd(self, reqId):
        #print(datetime.datetime.today(), threading.current_thread().name , "on_historicalDataEnd - ReqId:", reqId)
        
        ts = datetime.datetime.today()         
        self.fin_flux = self.fin_flux+1
        print(ts, "bot.fin_flux:", self.fin_flux, '/', "Requête ", reqId)

        DateFinD = datetime.datetime.strptime(self.TsFinStr, '%Y-%m-%d  %H:%M:%S').date()
        self.ecrire_fichier(reqId, DateFinD)


bot = Bot()
