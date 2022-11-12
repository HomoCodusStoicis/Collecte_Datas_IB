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

DateInDebStr="2022-11-04 00:00:00"

DateInFinStr = HierStr + " 23:59:59"
#DateInFinStr = "2022-03-19 03:59:59"

portProd = 7496
portSimu = 7497
portTWS=portSimu
import datetime
# DateStr = datetime.datetime.today().strftime("%Y-%m-%d")
# DateStr = "2021-12-23"

repOut = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\01 - Histo Bars Minutes'

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

    def  supprimer_Ordres_Parents(self,comment):
        
        timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y-%m-%d %H:%M:%S')
        print(timeStr, " - supprimer_Ordres_Parents" )

        F=(self.Histo_Croisements_Niveaux['OrderID']>0)
        for ID in self.Histo_Croisements_Niveaux.loc[F,'OrderID']:
            print("   Demande Cancel ordre", ID)
            self.Histo_Mes_Ordres.loc[ID, "Comment"] = comment
            self.Histo_Mes_Ordres.loc[ID, "TsExec"] = timeStr
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
        
        if status == "Cancelled":
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
                Resultat = (NbSortie * PrixSortie) + (NbEntree * PrixEntree)

                #Maj position :
                self.Histo_Mes_Ordres.loc[orderId,'Bilan'] = Resultat
                if Resultat > 0:
                    self.Histo_Mes_Ordres.loc[orderId,'Comment'] = 'OK'
                else:
                    self.Histo_Mes_Ordres.loc[orderId,'Comment'] = 'KO'

                #Maj position parente :
                self.Histo_Mes_Ordres.loc[parentId,'Bilan'] = Resultat
                if Resultat > 0:
                    self.Histo_Mes_Ordres.loc[parentId,'Comment'] = 'OK'
                else:
                    self.Histo_Mes_Ordres.loc[parentId,'Comment'] = 'KO'
                    
                
        
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
            else:
                print('   - Niveau supérieur : un ordre est déjà positionné sur le niveau {:s}'.format(nom_niveau_sup))
                
            # Si un ordre est déjà positionné, nul besoin de tout recalculer:
            if self.Histo_Croisements_Niveaux.loc[nom_niveau_inf,'OrderID'] == 0:
                # Si on a franchi le trigger :
                if self.ecart_niveau_inf <= self.ParamDistanceTriggers:
                    CreerOrdreInf=True
                    print('   - Niveau inférieur : le prix actuel est au-dessous du Trigger du niveau inférieur {:s} - Ordre LONGUE à positionner...'.format(nom_niveau_inf))
                else:
                    print("   - Niveau inférieur : le prix actuel est trop loin du niveau inférieur pour y placer un ordre LONG.")
            else:
                print('   - Niveau inférieur : un ordre est déjà positionné sur le niveau {:s}'.format(nom_niveau_inf))
            
 
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
  
        logging.error(" ReqId:", reqId,
              "Price:", price, "Size:", size, "Exch:" , exchange,
              "Spec Cond:", specialConditions, "PastLimit:", tickAtrribLast.pastLimit, "Unreported:", tickAtrribLast.unreported)
       
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
                self.supprimer_Ordres_Parents("Heure")

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
                    self.supprimer_Ordres_Parents("Débit")
                    
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

                    timeStr = datetime.datetime.fromtimestamp(tm.time()).strftime('%Y-%m-%d %H:%M:%S')
                    
                    #Boucle sur ces positions, pour les annuler :
                    for i in self.Histo_Mes_Ordres.loc[F1&F2&F3].index :
                        i_Sens      = self.Histo_Mes_Ordres.loc[i, "Sens"]
                        i_NomNiveau = self.Histo_Mes_Ordres.loc[i, "Niveau"]     
                        i_tickCross = self.Histo_Mes_Ordres.loc[i, "TickValidite"] - self.DureeVieOrdreParent_Ticks    
                        self.Histo_Mes_Ordres.loc[i, "Comment"] = "Range"
                        self.Histo_Mes_Ordres.loc[i, "TsExec"] = timeStr
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
            self.Histo_Mes_Ordres.to_csv(FichierHistoOrdresJour,sep=';',decimal='.',float_format='%.1f')
            
            self.reqOpenOrders()



                                          
            
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

 

    @iswrapper
    # ! [historicaldata]
    def historicalData(self, reqId:int, bar: BarData):
        #print(threading.current_thread().name , ts, "historicalData - ReqId:", reqId, bar.date)

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
        print(threading.current_thread().name , "Fin réception flux - HistoricalDataEnd - ReqId:", reqId, "from", start, "to", end)
        
        bot.on_historicalDataEnd(reqId)
   
    # ! [historicaldataend]

    @iswrapper
    # ! [historicalDataUpdate]
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
        #print(threading.current_thread().name , ts, "historicalDataUpdate - ReqId:", reqId, bar.date)
        
        bot.on_bar_update(reqId, bar)
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
    # ! [historicaltickslast]
    def historicalTicksLast(self, reqId: int, ticks: ListOfHistoricalTickLast,
                            done: bool):
        for tick in ticks:
            print("HistoricalTickLast. ReqId:", reqId, tick)
    # ! [historicaltickslast]



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
        self.cpt = pd.DataFrame(columns=['reqId','i','Contrat','Echeance','Periode','date','bar','BougieClotured','DateCouranteD'])
        self.cpt = self.cpt.astype({'reqId':numpy.int64, 'i':numpy.int64, 'Contrat':object, 'Echeance':object, 'Periode':object, 'BougieClotured':bool, 'DateCouranteD':object})
        # self.DateCourDt = datetime.datetime.strptime(DateStr, '%Y-%m-%d') 


    
    
        try:

               
            self.ib = IBApi()
    
            self.ib.connect("127.0.0.1", portTWS, clientId=0)
     
            logging.error("serverVersion:%s connectionTime:%s" % (self.ib.serverVersion(),
                                                          self.ib.twsConnectionTime()))
            #print("serverVersion:%s connectionTime:%s" % (ib.serverVersion(),
            #                                              ib.twsConnectionTime()))
        
                
            logging.error("Avant creation thread...")
            ib_thread = threading.Thread(target=self.run_loop, daemon=True)
            logging.error("Après creation thread...")
            ib_thread.start()
            logging.error("Après start thread...")
            
            # Start algo :
            logging.error("Dans start...")
            # tm.sleep(5)
            # logging.error("Dans start...après 5s")

               

    
            # ListeContratsIn = [["NQ",["202203"]],
            #                    ["YM"   ,["202203"]],
            #                    ["DXM"   ,["202203"]]
            #                  ]
            
            # ListeContrats = []
            # for i in ListeContratsIn:
            #     for j in i[1]:
            #         ListeContrats.append([i[0] , j])
            
            # print(ListeContrats)
            num_contrat_courant=0
            self.fin_flux = 0
            # Future_NomContrat=ListeContrats[num_contrat_courant][0]
            # Future_EcheanceContrat=ListeContrats[num_contrat_courant][1]


            self.ListeContrats   = ListeContrats
            self.contrat_courant=-1
            # JourneeDejaTraiteePourCeContrat = True
            
            #Passage au contrat suivant si pas tous déjà faits :
            while self.contrat_courant < len(self.ListeContrats) - 1:
                self.contrat_courant = self.contrat_courant + 1
                Future_NomContrat=self.ListeContrats[self.contrat_courant][0]
                Future_EcheanceContrat=self.ListeContrats[self.contrat_courant][1]
                print("----------------------")
                print("Start - Contrat suivant : " + Future_NomContrat + "- Ech"+ Future_EcheanceContrat)
                print("----------------------")

                self.ib.contract = self.ib.create_contract(Future_NomContrat, Future_EcheanceContrat)  # Create a contract
            
 
    
                self.ib.increment_id()  # Increment the order id
                #logging.error("self.ib.reqID:", self.ib.reqID)

        
                # if Mock_Api_Ib :
                #     DateFinDt = datetime.datetime.strptime(Mock_TS_Cour, '%Y%m%d %H:%M:%S')
                # else:
                #     DateFinDt=datetime.datetime.today()
                
                d = DateInFinStr
                DateFinStrQuery = d[0:4] + d[5:7] + d[8:10] + '-23:59:59'
                DateCourDt = DateInDebDt
                DateCourD = DateCourDt.date()
                # ProfondeurHistorique = "60 D"
                # #deltaDaysStr = str(deltaDays+1) + ' D'
                # #logging.error('Demande historique sur ', deltaDaysStr, "jusqu'au", DateFinStrQuery)
                
                # #queryTime = (datetime.datetime.today()).strftime("%Y%m%d %H:%M:%S")
                # self.ib.increment_id()
                # logging.error("Appel requete reqHistoricalData DAY..." + Future_NomContrat + "IdReq=" + str(self.ib.reqID))
                # self.cpt = self.cpt.append({'reqId':self.ib.reqID, 'i':0, 'Contrat': Future_NomContrat, 'Periode': '1D', 'BougieClotured':False}, ignore_index=True)

                # self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,
                #                   ProfondeurHistorique, "1 day", "TRADES", 0, 1, False, [])            


                # tm.sleep(3)
                # DateFinStrQuery=Mock_TS_Cour
                # DateFinStrQuery=(datetime.datetime.today()).strftime("%Y%m%d %H:%M:%S")
                
                ProfondeurHistorique = str( (DateInFinDt - DateInDebDt).days + 1 ) + ' D'
                print("Proondeur historique:",ProfondeurHistorique)
                self.ib.increment_id()
                logging.error("Appel requete reqHistoricalData 15 mins..." + Future_NomContrat + "IdReq=" + str(self.ib.reqID))
                cpt2 = pd.DataFrame.from_dict([{'reqId':self.ib.reqID, 'i':0, 'Contrat': Future_NomContrat, 'Echeance':Future_EcheanceContrat,
                                            'Periode': '15min', 'BougieClotured':False, 'DateCouranteD':DateCourD}])
               
                self.cpt = pd.concat([ cpt2, self.cpt], ignore_index=True)
                self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,
                                  ProfondeurHistorique, "15 mins", "TRADES", 0, 1, False, [])
                

                # ProfondeurHistorique = "25 D"
                self.ib.increment_id()
                logging.error("Appel requete reqHistoricalData 5 mins..." + Future_NomContrat + "IdReq=" + str(self.ib.reqID))
                cpt2 = pd.DataFrame.from_dict([{'reqId':self.ib.reqID, 'i':0, 'Contrat': Future_NomContrat, 'Echeance':Future_EcheanceContrat,
                                            'Periode': '5min', 'BougieClotured':False, 'DateCouranteD':DateCourD}])
              
                self.cpt = pd.concat([cpt2, self.cpt], ignore_index=True)
                self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,
                                  ProfondeurHistorique, "5 mins", "TRADES", 0, 1, False, [])

                # ProfondeurHistorique = "25 D"
                self.ib.increment_id()
                logging.error("Appel requete reqHistoricalData 1 min..." + Future_NomContrat + "IdReq=" + str(self.ib.reqID))
                cpt2 = pd.DataFrame.from_dict([{'reqId':self.ib.reqID, 'i':0, 'Contrat': Future_NomContrat, 'Echeance':Future_EcheanceContrat,
                                            'Periode': '1min', 'BougieClotured':False, 'DateCouranteD':DateCourD}])
                  
                self.cpt = pd.concat([cpt2, self.cpt], ignore_index=True)
                self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,
                                  ProfondeurHistorique, "1 min", "TRADES", 0, 1, False, [])

                # Activation du flux tick by tick :
                #self.ib.reqTickByTickData(self.ib.reqID, self.ib.contract , "Last", 0, True)
            
            #self.historicalTicksOperations()

            
            print("Executing requests ... finished")
    
    
            
        except:
            raise

            
        logging.error("Après lancement thread ib...")
        
        print(self.cpt)
    
    
    # def run_loop_mock(self):
    #     self.ib.run()   
        
        
    def run_loop(self):
        self.ib.run()        

    
    def on_bar_update(self, reqId, bar):
        
        ts = datetime.datetime.today()

        #if reading.currentthread().name , "on_bar_update - ReqId:", reqId, ' Bar:', bar)
        #Historisation de la barre si la bougie temps réelle est cloturée :
        F=(self.cpt['reqId'] == reqId)
        lastTs = self.cpt.loc[F,'date'].values[0]
        lastClose = self.cpt.loc[F,'bar'].values[0].close if not pd.isna(lastTs) else "0"
        Contrat = self.cpt.loc[F,'Contrat'].values[0]
        Periode = self.cpt.loc[F,'Periode'].values[0]
        
        # if Contrat == 'YM':
        #     print(ts, threading.current_thread().name , "on_bar_update - ReqId:", reqId, ' Bar:', bar)

        FlagAppelMajTabIndicateurs = False

        if self.fin_flux==12:
            
            #Bougie précédente cloturée et pas encore traitée:
            if not pd.isna(lastTs) and lastTs != bar.date and not self.cpt.loc[F,'BougieClotured'].values[0]:

                #On flag la req "bougie cloturée":
                self.cpt.loc[F,'BougieClotured'] = True


                #print("on_bar_update bar sur bougie cloturée")
                print("")
                print(ts, threading.current_thread().name , "on_bar_update sur bougie cloturée - ReqId:", reqId, lastTs)
                lastBar = self.cpt.loc[F,'bar'].values[0]
                # print(ts, "  dernière valeur connues : ", lastBar.date, "open:", lastBar.open, "close:",lastBar.close)

                # print(self.cpt)

                self.on_bar_update_histo(reqId, self.cpt.loc[F,'bar'].values[0] )
                #self.on_bar_update_histo(reqId, bar )
                
                #Maj EMA, RSI etc :
                self.maj_Indicateurs(reqId)

                PeriodeReq = self.cpt.loc[F,'Periode'].values[0]
                # print(ts, "  Période requete : ",PeriodeReq)
                #Mise à jour du tableau des indicateurs si tous les contrats ont été mis à jour pour cette unité de temps:
                F_ts=(self.cpt['date'] == lastTs)
                F_per=(self.cpt['Periode'] == PeriodeReq)
                F_1D=(self.cpt['Periode'] == '1D')
                F_15m=(self.cpt['Periode'] == '15min')
                F_5m=(self.cpt['Periode'] == '5min')
                F_1m=(self.cpt['Periode'] == '1min')
 
                # print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:")            
                # print(ts, "lastTs:", lastTs)            
                # print(self.cpt['Contrat'])
                # print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:")            
                # print(self.cpt['Contrat'].unique())
                # print("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx:")            
                NbContrats = len(self.cpt['Contrat'].unique())
                df_temp = self.cpt.loc[F_ts & F_per]
                df_temp_gb = df_temp.groupby(['Contrat'])
                NbContratsClotures = len(df_temp_gb['i'].last())
                # print(df_temp_gb['i'].last())
                # print(NbContratsClotures)
                if NbContratsClotures == NbContrats:

                    print(ts, "Bougie cloturée pour tous les contrats...")            
                
                    PeriodeHoraire15min = True if int(datetime.datetime.strptime(bar.date, '%Y%m%d %H:%M:%S').strftime("%M")) % 15 == 0 else False
                    PeriodeHoraire5min  = True if int(datetime.datetime.strptime(bar.date, '%Y%m%d %H:%M:%S').strftime("%M")) % 5  == 0 else False
                    F_BougieClotured = (self.cpt['BougieClotured'] == True)
                    #Si nouvelle période 15 minutes : on vérifie que les bougies 15min, 5min et 1min sont cloturées
                    if PeriodeHoraire15min:
                        print(ts, "Fin période 15min")            
                        NbBougiesCloturees = len(self.cpt.loc[F_BougieClotured & (F_15m | F_5m | F_1m)])
                        # print(ts, 'NbBougiesCloturees : ', NbBougiesCloturees)
                        if NbBougiesCloturees == 3* NbContrats:
                            FlagAppelMajTabIndicateurs = True                    #Si nouvelle période  5 minutes : on vérifie que les bougies 5min et 1min sont cloturées
                            PeriodeMaj='15min'
                            self.cpt.loc[(F_15m | F_5m | F_1m),'BougieClotured'] = False
                            self.cpt.loc[(F_15m | F_5m | F_1m),'date'] = bar.date
                            # print(self.cpt)
                    elif PeriodeHoraire5min:
                        print(ts, "Fin période 5min")            
                        NbBougiesCloturees = len(self.cpt.loc[F_BougieClotured & (F_5m | F_1m)])
                        print(ts, 'NbBougiesCloturees : ', NbBougiesCloturees)
                        if NbBougiesCloturees == 2* NbContrats: 
                            FlagAppelMajTabIndicateurs = True
                            PeriodeMaj='5min'
                            self.cpt.loc[(F_5m | F_1m),'BougieClotured'] = False
                            self.cpt.loc[(F_5m | F_1m),'date'] = bar.date
                            # print(self.cpt)
                    else: #periode1min
                        print(ts, "Fin période 1min")            
                        NbBougiesCloturees = len(self.cpt.loc[F_BougieClotured & (F_1m)])
                        print(ts, 'NbBougiesCloturees : ', NbBougiesCloturees)
                        if NbBougiesCloturees == NbContrats: 
                            FlagAppelMajTabIndicateurs = True
                            PeriodeMaj='1min'
                            self.cpt.loc[(F_1m),'BougieClotured'] = False
                            self.cpt.loc[(F_1m),'date'] = bar.date
                            # print(self.cpt)
                                                    


                if FlagAppelMajTabIndicateurs:
                    self.maj_TabIndicateurs(PeriodeMaj)
                    #print(Periode)
                    #On réinitialise le flag "bougie cloturée" pour toutes le requetes :
                    #self.cpt.loc[F,'BougieClotured'] = False




            #Mise à jour de la bougie courante si bougie non cloturée :
            if self.cpt.loc[F,'BougieClotured'].values[0] == False:
                self.cpt.loc[F,'date'] = bar.date
                self.cpt.loc[F,'bar'] = bar
                
        
    



    def on_bar_update_histo(self, reqId, bar):

        # self.Histo_ohlc_day = pd.DataFrame(columns=['Contrat','Date','open','high','low','close','Volume','EMA20','EMA50'])
        # self.Histo_ohlc_15min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50'])
        # self.Histo_ohlc_5min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50'])
        # self.Histo_ohlc_1min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50'])

        # print(reqId, bar)
        
        F=(self.cpt['reqId'] == reqId)
        self.cpt.loc[F,'i']=self.cpt.loc[F,'i']+1
        i_cpt =  self.cpt.loc[F,'i'].values[0]
        #print(i_cpt)

        Contrat = self.cpt.loc[F,'Contrat'].values[0]
        Periode = self.cpt.loc[F,'Periode'].values[0]
        Echeance = self.cpt.loc[F,'Echeance'].values[0]

           
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



        # Appel procédure d'écriture du fichier quotidien :
        DatePrecD =  self.cpt.loc[F,'DateCouranteD'].values[0]  
        DateCourD = date_ts.date()
        if DateCourD > DatePrecD:
            self.ecrire_fichier(reqId, DatePrecD)
            self.cpt.loc[F,'DateCouranteD'] = date_ts.date()


    def  ecrire_fichier(self, reqId, DateD):

        ts = datetime.datetime.today()         

        F=(self.cpt['reqId'] == reqId)
 
        Contrat = self.cpt.loc[F,'Contrat'].values[0]
        Periode = self.cpt.loc[F,'Periode'].values[0]
        Echeance = self.cpt.loc[F,'Echeance'].values[0]
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
                print("Fichier déjà existant : " +  ficOut )
                # print('Backup du fichier '+ ficOut + ' en ' + ficOutBackup)
                # shutil.move(ficOut, ficOutBackup)
                FichierATraiter = False
        except IOError:
                FichierATraiter = True
                
        if FichierATraiter:
            print("Ecriture fichier " + ficOut)

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
        
        ts = datetime.datetime.today()         
        self.fin_flux = self.fin_flux+1
        print(ts, "bot.fin_flux:", self.fin_flux, '/',len(self.cpt))


        # F=(self.cpt['reqId'] == reqId)
        DateFinD = datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d  %H:%M:%S').date()
        self.ecrire_fichier(reqId, DateFinD)


        if self.fin_flux == len(self.cpt):
            print(ts, "Toutes les requetes ont été traitées. FIN..." )
        
        # self.maj_Indicateurs(reqId)
        
 
        # tsStr = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H-%M-%S")

        # repertoireOut = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\02b - Histo HLC 15m-5m-1m'
    
        # F=(self.cpt['reqId'] == reqId)
        # iContrat = self.cpt.loc[F,'Contrat'].values[0]
        # iPeriode = self.cpt.loc[F,'Periode'].values[0]
        
        # if iPeriode=='15min':
        #     df=self.Histo_ohlc_15min.copy()
        # if iPeriode=='5min':
        #     df=self.Histo_ohlc_5min.copy()
        # if iPeriode=='1min':
        #     df=self.Histo_ohlc_1min.copy()
        
        
        # ficOut = repertoireOut + '\\Histo-' + iPeriode + '-' + iContrat + '-' + DateStr + '.csv'
        # ficOutBackup = repertoireOut + '\\Backup\\Histo-' + iPeriode + '-' + iContrat + '-' + DateStr + '-' + tsStr + '.bac'
        # # Test fichier resultat déjà existant :
        # try:
        #     with open(ficOut): 
        #         print("Fichier déjà existant : " +  ficOut )
        #         print('Backup du fichier '+ ficOut + ' en ' + ficOutBackup)
        #         shutil.move(ficOut, ficOutBackup)
        #         FichierATraiter = True
        # except IOError:
        #         FichierATraiter = True
                
        # if FichierATraiter:
        #     print("Ecriture fichier " + ficOut)
        #     F=(df['Contrat']==iContrat)
        #     df.loc[F].to_csv(ficOut,sep=';',decimal='.',float_format='%.1f', index=False)

        
        
            

    def  maj_Indicateurs(self, reqId):

        print(threading.current_thread().name , "maj_Indicateurs - ReqId:", reqId)


        F=(self.cpt['reqId'] == reqId)
        iContrat = self.cpt.loc[F,'Contrat'].values[0]
        iPeriode = self.cpt.loc[F,'Periode'].values[0]
        print("maj_Indicateurs ", iContrat, iPeriode)
        
        if iPeriode == "1D":

            #print(self.Histo_ohlc_day)
            F=(self.Histo_ohlc_day['Contrat']==iContrat)
            df0= self.Histo_ohlc_day.loc[F]  
            #print(df0)
            
            self.Histo_ohlc_day.loc[F,'EMA05'] = tb.MA(df0['close'],timeperiod=5,matype=1)
            self.Histo_ohlc_day.loc[F,'EMA20'] = tb.MA(df0['close'],timeperiod=20,matype=1)
            self.Histo_ohlc_day.loc[F,'EMA50'] = tb.MA(df0['close'],timeperiod=50,matype=1)
            self.Histo_ohlc_day.loc[F,'RSI14'] = tb.RSI(df0['close'],timeperiod=14)
    
    
        if iPeriode == "15min":

            F=(self.Histo_ohlc_15min['Contrat']==iContrat)
            df0= self.Histo_ohlc_15min.loc[F]  
            #print(df0)
            self.Histo_ohlc_15min.loc[F,'EMA05'] = tb.MA(df0['close'],timeperiod=5,matype=1)
            self.Histo_ohlc_15min.loc[F,'EMA20'] = tb.MA(df0['close'],timeperiod=20,matype=1)
            self.Histo_ohlc_15min.loc[F,'EMA50'] = tb.MA(df0['close'],timeperiod=50,matype=1)
            self.Histo_ohlc_15min.loc[F,'RSI14'] = tb.RSI(df0['close'],timeperiod=14)
            #print(self.Histo_ohlc_15min)
        if iPeriode == "5min":

            F=(self.Histo_ohlc_5min['Contrat']==iContrat)
            df0= self.Histo_ohlc_5min.loc[F]  
 
            self.Histo_ohlc_5min.loc[F,'EMA05'] = tb.MA(df0['close'],timeperiod=5,matype=1)
            self.Histo_ohlc_5min.loc[F,'EMA20'] = tb.MA(df0['close'],timeperiod=20,matype=1)
            self.Histo_ohlc_5min.loc[F,'EMA50'] = tb.MA(df0['close'],timeperiod=50,matype=1)
            self.Histo_ohlc_5min.loc[F,'RSI14'] = tb.RSI(df0['close'],timeperiod=14)

        if iPeriode == "1min":

            F=(self.Histo_ohlc_1min['Contrat']==iContrat)
            df0= self.Histo_ohlc_1min.loc[F]  

            self.Histo_ohlc_1min.loc[F,'EMA05'] = tb.MA(df0['close'],timeperiod=5,matype=1)
            self.Histo_ohlc_1min.loc[F,'EMA20'] = tb.MA(df0['close'],timeperiod=20,matype=1)
            self.Histo_ohlc_1min.loc[F,'EMA50'] = tb.MA(df0['close'],timeperiod=50,matype=1)
            self.Histo_ohlc_1min.loc[F,'RSI14'] = tb.RSI(df0['close'],timeperiod=14)


            
        
    def  maj_TabIndicateurs(self, opt):


        print("maj_TabIndicateurs opt: ", opt)
        
 
        if opt == 'init':
            print('dans init_TabIndicateurs...')
    
            # print(self.Histo_ohlc_day)
            # print(self.Histo_ohlc_15min)            
            # print(self.Histo_ohlc_5min)            
            # print(self.Histo_ohlc_1min)            

            self.TabIndicateurs = pd.DataFrame(columns=['ts','Contrat','KPI','1D','15min','5min','1min'])
            self.TabIndicateurs.set_index(['ts','Contrat','KPI'], inplace=True)
            self.TabHistoIndicateurs = pd.DataFrame(columns=['ts','Contrat','KPI','1D','15min','5min','1min'])
            self.TabHistoIndicateurs.set_index(['ts','Contrat','KPI'], inplace=True)
            self.NiveauxJ = pd.DataFrame()
            self.NiveauxJ = None
            
            for iContrat in self.Histo_ohlc_day['Contrat'].unique():
                print(iContrat)
                    
                #Récupération niveaux du jour :
                dic= {'YM':'DOW-mini', 'NQ':'NASDAQ-mini', 'DXM':'DAX-mini'}
                iContratLib = dic[iContrat]
                ficNiveauxJ = repNiveauxJ + "\\HistoHLCS_Pivots_Niveaux_" + iContratLib + "_" + DateStr + ".csv"
                try:
                    with open(ficNiveauxJ): 
                        #print("GetHisto = False")
                        FICNIVOK = True
                except IOError:
                        print("Fichier inxistant : " + ficNiveauxJ)
                        FICNIVOK = False   
                        quit()
    
                print('Chargement fichier des Niveaux du jour : ', ficNiveauxJ)
                iNiveauxJ = pd.read_csv(ficNiveauxJ, sep=';',decimal='.',parse_dates=['Date'])
                iNiveauxJ['Contrat'] = iContrat
                self.NiveauxJ = pd.concat([self.NiveauxJ, iNiveauxJ])
                
                
            self.NiveauxJ.reset_index(inplace=True)
            
            #print(self.NiveauxJ)            
            

        ts_i = (datetime.datetime.today()  + datetime.timedelta(minutes=-1)).strftime("%Y-%m-%d %H:%M") 
        

        if opt != 'init':
             self.TabIndicateurs.reset_index(inplace=True)
             self.TabIndicateurs['ts']=ts_i
             self.TabIndicateurs.set_index(['ts','Contrat','KPI'], inplace=True)
             
        
        # if  opt != 'init': 
        #     ts_last = self.TabIndicateurs.loc[-1,'ts'].values[0]
        #     print('ts_last:', ts_last)
        #     ts_i = '2021-12-05 20:00'
        #     self.TabIndicateurs.set_index(['ts'], inplace=True)
        #     self.TabIndicateurs.loc[ts_i]

        
        for iContrat in self.Histo_ohlc_day['Contrat'].unique():
            print(iContrat)

            
            KPI='LAST_BOUGIE'


            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                #print(lastLigne)
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['Date'] 
        
            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                #print(lastLigne)
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['Ts'] 

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                #print(lastLigne)
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['Ts'] 

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                #print(lastLigne)
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['Ts'] 


            KPI='LAST_PRICE'


            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['close'] 
        
            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['close'] 

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['close'] 

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['close'] 


            KPI='EMA20_Sup_EMA50'


            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['EMA20'] > lastLigne['EMA50']
        
            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]

                # print("lastLigne")
                # print(lastLigne)

                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['EMA20'] > lastLigne['EMA50']

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['EMA20'] > lastLigne['EMA50']

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['EMA20'] > lastLigne['EMA50']

            KPI='Prix_Sup_EMA50'

            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['close'] > lastLigne['EMA50']

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['close'] > lastLigne['EMA50']

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['close'] > lastLigne['EMA50']

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['close'] > lastLigne['EMA50']

            KPI='Pente_EMA50_5'
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-6]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = 10000*( lastLigne['EMA50'] - ligneRef['EMA50'] ) / ligneRef['EMA50']

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-6]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 10000*( lastLigne['EMA50'] - ligneRef['EMA50'] ) / ligneRef['EMA50']

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-6]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = 10000*( lastLigne['EMA50'] - ligneRef['EMA50'] ) / ligneRef['EMA50']

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-6]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = 10000*( lastLigne['EMA50'] - ligneRef['EMA50'] ) / ligneRef['EMA50']

            KPI='RSI14_SUP_50'
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['RSI14'] > 50

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['RSI14'] > 50

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['RSI14'] > 50

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['RSI14'] > 50

            KPI='Tendance_LT'

            for col in ['1D','15min','5min','1min']:
                

                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),col] = ''

                Tendance_LT_UP  = 0
                Tendance_LT_DOWN= 0
                
                if self.TabIndicateurs.loc[(ts_i, iContrat, 'EMA20_Sup_EMA50'),col] :
                    Tendance_LT_UP = Tendance_LT_UP + 1
                else:
                    Tendance_LT_DOWN = Tendance_LT_DOWN + 1
                
                if self.TabIndicateurs.loc[(ts_i, iContrat, 'Prix_Sup_EMA50'),col] :
                    Tendance_LT_UP = Tendance_LT_UP + 1
                else:
                    Tendance_LT_DOWN = Tendance_LT_DOWN + 1
                
                if self.TabIndicateurs.loc[(ts_i, iContrat, 'Pente_EMA50_5'),col] > 4 :
                    Tendance_LT_UP = Tendance_LT_UP + 1
                elif self.TabIndicateurs.loc[(ts_i, iContrat, 'Pente_EMA50_5'),col] < -4 :
                    Tendance_LT_DOWN = Tendance_LT_DOWN + 1
    
                if self.TabIndicateurs.loc[(ts_i, iContrat, 'RSI14_SUP_50'),col] :
                    Tendance_LT_UP = Tendance_LT_UP + 1
                else:
                    Tendance_LT_DOWN = Tendance_LT_DOWN + 1
    
                if Tendance_LT_UP >= 3:
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),col] = 'UP'
                if Tendance_LT_DOWN >= 3:
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),col] = 'DOWN'


            KPI='Pente_EMA05_2'
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = 10000*( lastLigne['EMA05'] - ligneRef['EMA05'] ) / ligneRef['EMA05']

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 10000*( lastLigne['EMA05'] - ligneRef['EMA05'] ) / ligneRef['EMA05']

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = 10000*( lastLigne['EMA05'] - ligneRef['EMA05'] ) / ligneRef['EMA05']

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = 10000*( lastLigne['EMA05'] - ligneRef['EMA05'] ) / ligneRef['EMA05']


            KPI='Couleur_Last_Bougie_HA'
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['xcouleur'] 

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['xcouleur'] 

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['xcouleur'] 

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['xcouleur'] 

            KPI='Nb_Bougies_meme_Couleur'
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                F2 = (df['xcouleur'] != lastLigne['xcouleur'] )
                NB1=df.index[-1]
                NB2=df.loc[F2].index[-1]
                # print('NB1=',NB1, ', NB2=', NB2)
                NB3=len(df.loc[NB2:NB1])-1
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = NB3
            
            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                F2 = (df['xcouleur'] != lastLigne['xcouleur'] )
                NB1=df.index[-1]
                NB2=df.loc[F2].index[-1]
                NB3=len(df.loc[NB2:NB1])-1
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = NB3

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                F2 = (df['xcouleur'] != lastLigne['xcouleur'] )
                NB1=df.index[-1]
                NB2=df.loc[F2].index[-1]
                NB3=len(df.loc[NB2:NB1])-1
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = NB3

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                F2 = (df['xcouleur'] != lastLigne['xcouleur'] )
                # print("==="*50)
                # print(iContrat)
                # print(lastLigne['xcouleur'] )
                # print(df)
                NB1=df.index[-1]
                NB2=df.loc[F2].index[-1]
                # print('NB1=',df.index[-1], df.loc[NB1,'Ts'])
                # print('NB2=',df.loc[F2].index[-1], df.loc[NB2,'Ts'])
                NB3=len(df.loc[NB2:NB1])-1
                # print('NB3=',NB3)
                print(df.loc[NB2:NB1])
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = NB3


            KPI='Tendance_CT'

            for col in ['1D','15min','5min','1min']:
                

                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),col] = ''

                Tendance_CT_UP   = 0
                Tendance_CT_DOWN = 0
                
                
                if self.TabIndicateurs.loc[(ts_i, iContrat, 'Pente_EMA05_2'),col] > 4 :
                    Tendance_CT_UP = Tendance_CT_UP + 1
                elif self.TabIndicateurs.loc[(ts_i, iContrat, 'Pente_EMA05_2'),col] < -4 :
                    Tendance_CT_DOWN = Tendance_CT_DOWN + 1
    
                if self.TabIndicateurs.loc[(ts_i, iContrat, 'Couleur_Last_Bougie_HA'),col] == 'Green' :
                    Tendance_CT_UP = Tendance_CT_UP + 1
                    if self.TabIndicateurs.loc[(ts_i, iContrat, 'Nb_Bougies_meme_Couleur'),col] > 2 :
                        Tendance_CT_UP = Tendance_CT_UP + 1
                elif self.TabIndicateurs.loc[(ts_i, iContrat, 'Couleur_Last_Bougie_HA'),col] == 'Red' :
                    Tendance_CT_DOWN = Tendance_CT_DOWN + 1
                    if self.TabIndicateurs.loc[(ts_i, iContrat, 'Nb_Bougies_meme_Couleur'),col] > 2 :
                        Tendance_CT_DOWN = Tendance_CT_DOWN + 1
    
    
                if Tendance_CT_UP >= 2:
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),col] = 'UP'
                if Tendance_CT_DOWN >= 2:
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),col] = 'DOWN'

            # print("avant KPI Scénario_Tendance")            
            # print(self.TabIndicateurs)

            KPI='Scénario_Tendance'

            for col in ['1D','15min','5min','1min']:
                

                if self.TabIndicateurs.loc[(ts_i, iContrat, 'Tendance_LT'),col] == 'UP' :
                    
                    if self.TabIndicateurs.loc[(ts_i, iContrat, 'Tendance_CT'),col] == 'UP' :
                        
                        self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),col] = 'UP - Continuité' 
                        
                    elif self.TabIndicateurs.loc[(ts_i, iContrat, 'Tendance_CT'),col] == 'DOWN' : 
                   
                        self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),col] = 'UP - Respiration' 
                        
                    else:
                        self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),col] = 'UP - Accumulation'
                
                
                if self.TabIndicateurs.loc[(ts_i, iContrat, 'Tendance_LT'),col] == 'DOWN' :
                    
                    if self.TabIndicateurs.loc[(ts_i, iContrat, 'Tendance_CT'),col] == 'DOWN' :
                        
                        self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),col] = 'DOWN - Continuité' 
                        
                    elif self.TabIndicateurs.loc[(ts_i, iContrat, 'Tendance_CT'),col] == 'UP' : 
                   
                        self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),col] = 'DOWN - Respiration' 
                        
                    else:
                        self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),col] = 'DOWN - Accumulation'


            KPI='Dist_EMA20'
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['close'] - lastLigne['EMA20']

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['close'] - lastLigne['EMA20']

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['close'] - lastLigne['EMA20']

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['close'] - lastLigne['EMA20']


            KPI='Dist_EMA50'
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['close'] - lastLigne['EMA50']

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['close'] - lastLigne['EMA50']

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['close'] - lastLigne['EMA50']

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['close'] - lastLigne['EMA50']
                

            KPI='Dist_Niveaux'  
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                lastPrix = lastLigne['close']
                # print("lastPrix:",lastPrix)
                F2 = (self.NiveauxJ['Contrat'] == iContrat)
                iNiv= self.NiveauxJ.loc[F2]

                # print("iNiv")
                # print(iNiv)
                F_Res = (iNiv['Prix'] >= lastPrix)
                # print("iNiv-Res")
                # print(iNiv.loc[F_Res])
                for r in [0,1,2]:
                    Rr = iNiv.loc[F_Res].iloc[r]
                    Ecart = '{:0.1f}'.format(Rr['Prix'] - lastPrix )
                    Rr_desc = [Rr['Niveau'], Rr['Prix'], Ecart ]
                    KPI='R'+str(r)
                    # print("KPI : " + KPI)
                    # print("Rr_desc:",Rr_desc)
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = 'Init'
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = Rr_desc

                F_Sup = (iNiv['Prix'] <= lastPrix)
                # print("iNiv-Sup")
                # print(iNiv.loc[F_Sup])
                for s in [-1,-2,-3]:
                    Ss = iNiv.loc[F_Sup].iloc[s]
                    # print(iNiv.loc[F_Sup])
                    Ecart = '{:0.1f}'.format(lastPrix - Ss['Prix'] )
                    Ss_desc = [Ss['Niveau'], Ss['Prix'], Ecart]
                    KPI='S'+str(-s-1)
                    # print("KPI : " + KPI)
                    # print("Ss_desc:",Ss_desc)
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = 'Init'
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = Ss_desc

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                lastPrix = lastLigne['close']
                # print("lastPrix:",lastPrix)
                F2 = (self.NiveauxJ['Contrat'] == iContrat)
                iNiv= self.NiveauxJ.loc[F2]

                # print("iNiv")
                # print(iNiv)
                F_Res = (iNiv['Prix'] >= lastPrix)
                # print("iNiv-Res")
                # print(iNiv.loc[F_Res])
                for r in [0,1,2]:
                    Rr = iNiv.loc[F_Res].iloc[r]
                    Ecart = '{:0.1f}'.format(Rr['Prix'] - lastPrix )
                    Rr_desc = [Rr['Niveau'], Rr['Prix'], Ecart ]
                    KPI='R'+str(r)
                    # print("KPI : " + KPI)
                    # print("Rr_desc:",Rr_desc)
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 'Init'
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = Rr_desc

                F_Sup = (iNiv['Prix'] <= lastPrix)
                # print("iNiv-Sup")
                # print(iNiv.loc[F_Sup])
                for s in [-1,-2,-3]:
                    Ss = iNiv.loc[F_Sup].iloc[s]
                    # print(iNiv.loc[F_Sup])
                    Ecart = '{:0.1f}'.format(lastPrix - Ss['Prix'] )
                    Ss_desc = [Ss['Niveau'], Ss['Prix'], Ecart]
                    KPI='S'+str(-s-1)
                    # print("KPI : " + KPI)
                    # print("Ss_desc:",Ss_desc)
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 'Init'
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = Ss_desc

            if opt in ['init','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                lastPrix = lastLigne['close']
                # print("lastPrix:",lastPrix)
                F2 = (self.NiveauxJ['Contrat'] == iContrat)
                iNiv= self.NiveauxJ.loc[F2]

                # print("iNiv")
                # print(iNiv)
                F_Res = (iNiv['Prix'] >= lastPrix)
                # print("iNiv-Res")
                # print(iNiv.loc[F_Res])
                for r in [0,1,2]:
                    Rr = iNiv.loc[F_Res].iloc[r]
                    Ecart = '{:0.1f}'.format(Rr['Prix'] - lastPrix )
                    Rr_desc = [Rr['Niveau'], Rr['Prix'], Ecart ]
                    KPI='R'+str(r)
                    # print("KPI : " + KPI)
                    # print("Rr_desc:",Rr_desc)
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = 'Init'
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = Rr_desc

                F_Sup = (iNiv['Prix'] <= lastPrix)
                # print("iNiv-Sup")
                # print(iNiv.loc[F_Sup])
                for s in [-1,-2,-3]:
                    Ss = iNiv.loc[F_Sup].iloc[s]
                    # print(iNiv.loc[F_Sup])
                    Ecart = '{:0.1f}'.format(lastPrix - Ss['Prix'] )
                    Ss_desc = [Ss['Niveau'], Ss['Prix'], Ecart]
                    KPI='S'+str(-s-1)
                    # print("KPI : " + KPI)
                    # print("Ss_desc:",Ss_desc)
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = 'Init'
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = Ss_desc

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                lastPrix = lastLigne['close']
                # print("lastPrix:",lastPrix)
                F2 = (self.NiveauxJ['Contrat'] == iContrat)
                iNiv= self.NiveauxJ.loc[F2]

                # print("iNiv")
                # print(iNiv)
                F_Res = (iNiv['Prix'] >= lastPrix)
                # print("iNiv-Res")
                # print(iNiv.loc[F_Res])
                for r in [0,1,2]:
                    Rr = iNiv.loc[F_Res].iloc[r]
                    Ecart = '{:0.1f}'.format(Rr['Prix'] - lastPrix )
                    Rr_desc = [Rr['Niveau'], Rr['Prix'], Ecart ]
                    KPI='R'+str(r)
                    # print("KPI : " + KPI)
                    # print("Rr_desc:",Rr_desc)
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = 'Init'
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = Rr_desc

                F_Sup = (iNiv['Prix'] <= lastPrix)
                # print("iNiv-Sup")
                # print(iNiv.loc[F_Sup])
                for s in [-1,-2,-3]:
                    Ss = iNiv.loc[F_Sup].iloc[s]
                    # print(iNiv.loc[F_Sup])
                    Ecart = '{:0.1f}'.format(lastPrix - Ss['Prix'] )
                    Ss_desc = [Ss['Niveau'], Ss['Prix'], Ecart]
                    KPI='S'+str(-s-1)
                    # print("KPI : " + KPI)
                    # print("Ss_desc:",Ss_desc)
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = 'Init'
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = Ss_desc





        #self.TabHistoIndicateurs = pd.concat([self.TabHistoIndicateurs, self.TabIndicateurs])
        ts_str = datetime.datetime.today()  
        
        print('==='*50)
        print('   '*10,' TabIndicateurs: ',ts_str)
        print('==='*50)
        print(self.TabIndicateurs)
        # print('TabHistoIndicateurs:')
        # print(self.TabHistoIndicateurs)
        

bot = Bot()
