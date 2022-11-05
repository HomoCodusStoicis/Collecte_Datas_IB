"""
Copyright (C) 2019 Interactive Brokers LLC. All rights reserved. This code is subject to the terms
 and conditions of the IB API Non-Commercial License or the IB API Commercial License, as applicable.
"""

Mock_Api_Ib = False
Mock_TS_Cour = "2022-03-10 16:00:00"
Mock_Alerte = False
# Mock_TS_Cour = "2022-01-13 17:45:00"


# Pour notifications whatsapp (utilisation API twilio)
import os
from twilio.rest import Client 
os.environ['TWILIO_ACCOUNT_SID']='AC33df42051033b8da4eabdcb659eadcc3'
os.environ['TWILIO_AUTH_TOKEN']='2d6eba7ad1525258893df90a31db15ef'


portProd = 7496
portSimu = 7497
portTWS=portSimu
import datetime
if Mock_Api_Ib:
    DateStr = Mock_TS_Cour[0:10]
else:
    DateStr = datetime.datetime.today().strftime("%Y-%m-%d")
# DateStr = "2021-12-30"

repNiveauxJ = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\06 - Niveaux enrichis'

import argparse

import collections
import inspect
from ast import literal_eval

import logging
import time as tm
import os.path

import threading

from scipy.stats.mstats import gmean
import talib as tb

#Bouchon des APIs IB
import Mock_Api_IB

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
from OrderSamples import OrderSamples

# Librairies persos
import Niveaux, Horaires

import pandas as pd
from pandas.api.types import CategoricalDtype


# pour faire du postgresql :
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
from io import StringIO
import sys
import glob

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 3000)
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
    console.setLevel(logging.DEBUG)
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
        #print(threading.currentThread().getName() , ts, "historicalData - ReqId:", reqId, bar.date)

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
            isPeriodeCourante15min = delta_time < datetime.timedelta(minutes=15+1)
            isPeriodeCourante5min  = delta_time < datetime.timedelta(minutes=5+1)
            isPeriodeCourante1min  = delta_time < datetime.timedelta(minutes=1+1)
        
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
        print(threading.currentThread().getName() , "Fin réception flux - HistoricalDataEnd - ReqId:", reqId, "from", start, "to", end)
        
        bot.on_historicalDataEnd(reqId)
   
    # ! [historicaldataend]

    @iswrapper
    # ! [historicalDataUpdate]
    def historicalDataUpdate(self, reqId: int, bar: BarData):
        ts = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H:%M:%S")
        #print(threading.currentThread().getName() , ts, "historicalDataUpdate - ReqId:", reqId, bar.date)
        
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
        self.Histo_ohlc_15min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50','EMA05','RSI14','xopen','xclose','xlow','xhigh','xcouleur'])
        #self.Histo_ohlc_15min.set_index(["Contrat","Ts"], inplace=True)
        self.Histo_ohlc_5min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50','EMA05','RSI14','xopen','xclose','xlow','xhigh','xcouleur'])
        #self.Histo_ohlc_5min.set_index(["Contrat","Ts"], inplace=True)
        self.Histo_ohlc_1min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50','EMA05','RSI14','xopen','xclose','xlow','xhigh','xcouleur'])
        #self.Histo_ohlc_1min.set_index(["Contrat","Ts"], inplace=True)
        self.i_day = 0
        self.i_15min=0
        self.i_5min=0
        self.i_1min=0
        self.i_day_2 = 0
        self.cpt = pd.DataFrame(columns=['reqId','i','Contrat','Periode','TsBougieCouranteStr','bar','BougieClotured','UpdateStarted','TsClotureBougieCouranteStr','TsClotureNextBougieStr'])
        self.HistoMinMaxDf   = pd.DataFrame()
        self.SuiviGlobalDf   = pd.DataFrame()
        self.SuiviStrategies = pd.DataFrame(columns=['Ts','TsBougie','Contrat','TailleBougie','Sens', 'Strategie','Probabilité','RemPour','RemContre'])
    
        try:

            if Mock_Api_Ib == False:
                
                self.ib = IBApi()
        
                self.ib.connect("127.0.0.1", portTWS, clientId=0)
         
                logging.error("serverVersion:%s connectionTime:%s" % (self.ib.serverVersion(),
                                                              self.ib.twsConnectionTime()))
                #print("serverVersion:%s connectionTime:%s" % (ib.serverVersion(),
                #                                              ib.twsConnectionTime()))
        
            else:
                
                self.ib = Mock_Api_IB.Mock_IBApi(self)
                
            logging.error("Avant creation thread...")
            ib_thread = threading.Thread(target=self.run_loop, daemon=True)
            logging.error("Après creation thread...")
            ib_thread.start()
            logging.error("Après start thread...")
            
            # Start algo :
            logging.error("Dans start...")
            tm.sleep(5)
            logging.error("Dans start...après 5s")

               

    
            ListeContratsIn = [["NQ"  ,["202206"]],
                               ["YM"  ,["202206"]],
                               ["DXM" ,["202206"]]
                             ]
            
            ListeContrats = []
            for i in ListeContratsIn:
                for j in i[1]:
                    ListeContrats.append([i[0] , j])
            
            print(ListeContrats)
            num_contrat_courant=0
            self.fin_flux = 0
            # Future_NomContrat=ListeContrats[num_contrat_courant][0]
            # Future_EcheanceContrat=ListeContrats[num_contrat_courant][1]

 
            while num_contrat_courant < len(ListeContrats) :
                Future_NomContrat=ListeContrats[num_contrat_courant][0]
                Future_EcheanceContrat=ListeContrats[num_contrat_courant][1]
                num_contrat_courant = num_contrat_courant + 1
                logging.error("Start - Contrat suivant : " + Future_NomContrat + "- Ech"+ Future_EcheanceContrat)
                self.ib.contract = self.ib.create_contract(Future_NomContrat, Future_EcheanceContrat)  # Create a contract
            
 
    
                self.ib.increment_id()  # Increment the order id
                #logging.error("self.ib.reqID:", self.ib.reqID)

        
                if Mock_Api_Ib :
                    DateFinDt = datetime.datetime.strptime(Mock_TS_Cour, '%Y-%m-%d %H:%M:%S')
                else:
                    DateFinDt=datetime.datetime.today()
                
                DateFinStrQuery=DateFinDt.strftime('%Y%m%d %H:%M:%S') 

                ProfondeurHistorique = "60 D"
                #deltaDaysStr = str(deltaDays+1) + ' D'
                #logging.error('Demande historique sur ', deltaDaysStr, "jusqu'au", DateFinStrQuery)
                
                #queryTime = (datetime.datetime.today()).strftime("%Y%m%d %H:%M:%S")
                self.ib.increment_id()
                logging.error("Appel requete reqHistoricalData DAY..." + Future_NomContrat + "IdReq=" + str(self.ib.reqID))
                self.cpt = self.cpt.append({'reqId':self.ib.reqID, 'i':0, 'Contrat': Future_NomContrat, 'Periode': '1D', 'BougieClotured':False}, ignore_index=True)

                self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,
                                  ProfondeurHistorique, "1 day", "TRADES", 0, 1, False, [])            


                # tm.sleep(3)
                DateFinStrQuery=''
                ProfondeurHistorique = "5 D"
                self.ib.increment_id()
                logging.error("Appel requete reqHistoricalData 15 mins..." + Future_NomContrat + "IdReq=" + str(self.ib.reqID))
                self.cpt = self.cpt.append({'reqId':self.ib.reqID, 'i':0, 'Contrat': Future_NomContrat, 'Periode': '15min', 'BougieClotured':False}, ignore_index=True)
                self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,
                                  ProfondeurHistorique, "15 mins", "TRADES", 0, 1, True, [])
                

                ProfondeurHistorique = "4 D"
                self.ib.increment_id()
                logging.error("Appel requete reqHistoricalData 5 mins..." + Future_NomContrat + "IdReq=" + str(self.ib.reqID))
                self.cpt = self.cpt.append({'reqId':self.ib.reqID, 'i':0, 'Contrat': Future_NomContrat, 'Periode': '5min', 'BougieClotured':False}, ignore_index=True)
                self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,
                                  ProfondeurHistorique, "5 mins", "TRADES", 0, 1, True, [])

                ProfondeurHistorique = "15000 S"
                self.ib.increment_id()
                logging.error("Appel requete reqHistoricalData 1 min..." + Future_NomContrat + "IdReq=" + str(self.ib.reqID))
                self.cpt = self.cpt.append({'reqId':self.ib.reqID, 'i':0, 'Contrat': Future_NomContrat, 'Periode': '1min', 'BougieClotured':False}, ignore_index=True)
                self.ib.reqHistoricalData(self.ib.reqID, self.ib.contract, DateFinStrQuery,
                                  ProfondeurHistorique, "1 min", "TRADES", 0, 1, True, [])

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

    
    def on_bar_clotured(self, reqId, bar, TsClotureBougieDt):

        ts = datetime.datetime.today()

        # print(ts, threading.currentThread().getName() , "on_bar_clotured - ReqId:", reqId, ' Bar:', bar)

        self.on_bar_update_histo(reqId, bar )
        #self.on_bar_update_histo(reqId, bar )
        
        #Maj EMA, RSI etc :
        self.maj_Indicateurs(reqId)
        F=(self.cpt['reqId'] == reqId)
        PeriodeReq = self.cpt.loc[F,'Periode'].values[0]
        TsClotureBougieStr = TsClotureBougieDt.strftime('%Y%m%d %H:%M:%S')


        self.cpt['AffichageAsked'] = False
        self.cpt.loc[F,['AffichageAsked']] = True

        self.maj_TabIndicateurs(PeriodeReq)
        
        self.cpt.loc[F,['BougieClotured']] = False
        self.cpt.loc[F,['TsClotureBougieCouranteStr']] = self.cpt.loc[F,['TsClotureNextBougieStr']]


        intPeriode = 15 if PeriodeReq=='15min' else 5 if PeriodeReq=='5min' else 1
        TsBougieNextDt     = TsClotureBougieDt + datetime.timedelta(minutes=intPeriode)
        self.cpt.loc[F,['TsClotureNextBougieStr']] = TsBougieNextDt.strftime('%Y%m%d %H:%M:%S')
        
       
        self.evaluer_strategies(reqId)


    def evaluer_strategies(self, reqId):

        print(datetime.datetime.today(), "evaluer_strategies", reqId)


        F=(self.cpt['reqId'] == reqId)
        TailleBougie = self.cpt.loc[F,'Periode'].values[0]
        Contrat      = self.cpt.loc[F,'Contrat'].values[0]
        TsBougieStr = self.cpt.loc[F,'TsBougieCouranteStr'].values[0]

        F_c  = (self.SuiviGlobalDf['Contrat']==Contrat)
        F_tb = (self.SuiviGlobalDf['TailleBougie']==TailleBougie)
        Tendance_LT_actuelle = self.SuiviGlobalDf.loc[F_c & F_tb].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'Tendance_LT']
        Tendance_CT_actuelle = self.SuiviGlobalDf.loc[F_c & F_tb].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'Tendance_CT']
        Distance_EMA20 = float(self.SuiviGlobalDf.loc[F_c & F_tb].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'Dist_EMA20'])
        Distance_EMA50 = float(self.SuiviGlobalDf.loc[F_c & F_tb].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'Dist_EMA50'])
        Distance_R0 =  float(self.SuiviGlobalDf.loc[F_c & F_tb].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'R0'][2])
        Distance_S0 =  float(self.SuiviGlobalDf.loc[F_c & F_tb].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'S0'][2])
        R0 = self.SuiviGlobalDf.loc[F_c & F_tb].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'R0'][0]
        S0 = self.SuiviGlobalDf.loc[F_c & F_tb].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'S0'][0]  
        
        F_c  = (self.HistoMinMaxDf['Contrat']==Contrat)
        F_tb = (self.HistoMinMaxDf['TailleBougie']==TailleBougie)
        F_type=(self.HistoMinMaxDf['Type']=='Min')
        ListeFreinsDernierMin = self.HistoMinMaxDf.loc[F_c & F_tb & F_type].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'ListeFreins']
        F_type=(self.HistoMinMaxDf['Type']=='Max')
        ListeFreinsDernierMax = self.HistoMinMaxDf.loc[F_c & F_tb & F_type].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'ListeFreins']
        NbCulsPlatsTendanceAvantDernierPic = self.HistoMinMaxDf.loc[F_c & F_tb & F_type].sort_values(by=['Ts'], ascending=False).reset_index().loc[0,'NbCulsPlats']
        #----------------------------------------------------------------------------------------------------#
        #  Strategie S1 : suivi de tendance : entrée sur début retracement suite à une respiration           #
        #----------------------------------------------------------------------------------------------------#
        #  Conditions d'éligibilité :                                                                        #
        #               1/  Tendance marquée :                                                               #
        #                   - tendance LT UP ou DOWN mais non flat, avec au moins 3 culs plats               #
        #                   - au moins déjà un rebond sur EMA20 ou EMA50                                     #
        #               2/  Possibilité de rebond : on se rapproche de EMA20/EMA50, via tend CT contraire    #
        #               3/  Eventuellement possibilité de rebond sur une autre ligne                         #
        #               4/  Pas de frein majeur qui viendrait entraver la poursuite de la tendance           #
        #----------------------------------------------------------------------------------------------------#
        C1, C2, C3, C4 = False, False, False, False
        RemPour, RemContre = "", ""

        if Tendance_LT_actuelle == 'UP' and NbCulsPlatsTendanceAvantDernierPic >= 3 :
            #le dernier MIN s'explique par une MM ?
            if 'EMA20' in ListeFreinsDernierMin or 'EMA50' in ListeFreinsDernierMin:
                C1 = True
       
        if Tendance_LT_actuelle == 'DOWN' and NbCulsPlatsTendanceAvantDernierPic >= 3 :
            #le dernier MAX s'explique par une MM ?
            if 'EMA20' in ListeFreinsDernierMax or 'EMA50' in ListeFreinsDernierMax:
                C1 = True
                
        if C1:
            
            # La tendance CT est à contre-courant  
            if Tendance_LT_actuelle == 'UP' and Tendance_CT_actuelle == 'DOWN' and (Distance_EMA20 < 20 or Distance_EMA50 < 20):
                C2 = True
                
        if C1 and C2:
            
            if Tendance_LT_actuelle == 'UP':

                # Présence d'un support qui pourrait aider le rebond :
                if Distance_S0 < 20 and Distance_S0 > 0:
                    C3 = True
                    RemPour = RemPour + 'Support en + pour amplifier rebond : ' + S0 + ". "

                # Présence d'une résistance qui pourrait entraver le rebond :
                if Distance_R0 < 20 and Distance_R0 > 0:
                    C4= True
                    RemContre = RemContre + 'Resistance qui pourrait freiner rebond : ' + R0 + ". "
                else:
                    RemPour = RemPour + 'Pas de resistance qui pourrait freiner rebond' + ". "
                    
            if Tendance_LT_actuelle == 'DOWN':
                # Présence d'une résistance qui pourrait aider le rebond :
                if Distance_R0 < 20 and Distance_R0 > 0:
                    C3 = True
                    RemPour = RemPour + 'Resistance en + pour amplifier rebond : ' + R0 + ". "

                # Présence d'un support qui pourrait entraver le rebond :
                if Distance_S0 < 20 and Distance_S0 > 0:
                    C4 = True
                    RemContre = RemContre + 'Support qui pourrait freiner rebond : ' + S0 + ". "
                else:
                    RemPour = RemPour + 'Pas de Support qui pourrait freiner rebond' + ". "
        
        
        p=0
        if C1 and C2:
            p = 0.5
            if C3:
                p = p + 0.2
            if not C4:
                p = p + 0.2
            
            self.SuiviStrategies =  self.SuiviStrategies.append({'Ts':datetime.datetime.today(), 'TsBougie': TsBougieStr, 'Contrat':Contrat, 'TailleBougie': TailleBougie , 
                                   'Sens' : Tendance_LT_actuelle, 'Strategie':'S1 : suivi de tendance : entrée sur rebond MM suite à une respiration ', 'Probabilité':p, 'RemPour': RemPour,
                                   'RemContre':RemContre}, ignore_index=True)
            
            TexteAlerte = TsBougieStr + " - "  + Contrat + " - " + TailleBougie  + " - " + Tendance_LT_actuelle + '- Strategie:S1 : suivi de tendance : entrée sur rebond MM suite à une respiration '
    
            # Strategie S4 : Bougie d'invalidation HA après une tendance marquée         
            
            if not Mock_Alerte and  TailleBougie != '1min':
                self.Creer_Alerte(TexteAlerte)
            
            self.Store_SuiviStrategies()


    def Creer_Alerte(self,TexteAlerte):
        
        client = Client() 
        # c'est le numéro de test de la sandbox WhatsApp
        from_whatsapp_number='whatsapp:+14155238886' 
        # remplacez ce numéro avec votre propre numéro WhatsApp
        to_whatsapp_number='whatsapp:+33686784587' 
        client.messages.create(body=TexteAlerte, from_=from_whatsapp_number,to=to_whatsapp_number)
            
            
        


    def Store_SuiviStrategies(self):
        # ts = datetime.datetime.today()
        # print(ts, "Debut Store_SuiviStrategies ...")


        
        # Define a function that handles and parses psycopg2 exceptions
        def show_psycopg2_exception(err):
            # get details about the exception
            err_type, err_obj, traceback = sys.exc_info()    
            # get the line number when exception occured
            line_n = traceback.tb_lineno    
            # print the connect() error
            print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
            print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
            # psycopg2 extensions.Diagnostics object attribute
            print ("\nextensions.Diagnostics:", err.diag)    
            # print the pgcode and pgerror exceptions
            print ("pgerror:", err.pgerror)
            print ("pgcode:", err.pgcode, "\n")
            
        # Define function using copy_from() with StringIO to insert the dataframe
        def copy_from_dataFile_StringIO(conn, datafrm, table):
            
          # save dataframe to an in memory buffer
            buffer = StringIO()
            datafrm.to_csv(buffer, header=False, index = False, sep=';',decimal='.')
            buffer.seek(0)
            
            cursor = conn.cursor()
            try:
                cursor.copy_from(buffer, table, sep=';', null="")
                # print("Data inserted using copy_from_datafile_StringIO() successfully....")
            except (Exception, psycopg2.DatabaseError) as err:
                # pass exception to function
                show_psycopg2_exception(err)
                cursor.close()
            conn.commit()
            cur.close()
        
        conn = psycopg2.connect("host= 127.0.0.1 dbname=Trading user=postgres password=admin")
        #"host= 127.0.0.1 dbname=testdb user=postgres password=postgres")
        # print("Connecting to Database")
        # FichierJ = csvPath + "SuiviTotalDf.csv"
        # df = pd.read_csv(FichierJ, sep=';',decimal='.')

        #Suppression des lignes :
        sqlQueryDelete = 'DELETE FROM "SuiviStrategies" ;\n '
        cur = conn.cursor()
        cur.execute(sqlQueryDelete)
        conn.commit()
        # print("Vidage table SuiviStrategies OK...")

        F = (self.SuiviStrategies['Probabilité']>0)
        df=self.SuiviStrategies.loc[F]
        print(df)
        
        copy_from_dataFile_StringIO(conn, df, "SuiviStrategies")




    def on_bar_update(self, reqId, bar):
        
        ts = datetime.datetime.today()

        # print(ts, threading.currentThread().getName() , "on_bar_update - ReqId:", reqId, ' Bar:', bar)
        #Historisation de la barre si la bougie temps réelle est cloturée :
        F=(self.cpt['reqId'] == reqId)
        # print( self.cpt.loc[F])
        lastTs = self.cpt.loc[F,'TsBougieCouranteStr'].values[0]
        # print("self.cpt.loc[F,'TsBougieCouranteStr'].values[0]:", self.cpt.loc[F,'TsBougieCouranteStr'].values[0])
        # print("lastTs:", lastTs)
        # print("lastTs-1:", bar.date+ datetime.timedelta(minutes=1))
        lastClose = self.cpt.loc[F,'bar'].values[0].close if not pd.isna(lastTs) else "0"
        Contrat = self.cpt.loc[F,'Contrat'].values[0]
        Periode = self.cpt.loc[F,'Periode'].values[0]
        
        # if Contrat == 'YM':
        #     print(ts, threading.currentThread().getName() , "on_bar_update - ReqId:", reqId, ' Bar:', bar)

        FlagAppelMajTabIndicateurs = False


        # if Mock_Api_Ib and pd.isna(lastTs):
        #     self.fin_flux = 12
            # print("Type de lastTs:", lastTs.dtype)
            # lastTsDt      = datetime.datetime.strptime(bar.date, "%Y%m%d %H:%M:%S")  
            # lastTs        = (lastTsDt + datetime.timedelta(minutes=-1)).strftime('%Y%m%d %H:%M:%S')
            
            
            

        if self.fin_flux==12:
            
            #Bougie précédente cloturée et pas encore traitée:
            if not pd.isna(lastTs) and lastTs != bar.date and not self.cpt.loc[F,'BougieClotured'].values[0] and self.cpt.loc[F,'UpdateStarted'].values[0]:

                #On flag la req "bougie cloturée":
                # self.cpt.loc[F,'BougieClotured'] = True
                # self.cpt.loc[F,'TsClotureNextBougieStr'] = bar.date
                TsClotureBougieCouranteStr =  self.cpt.loc[F,['TsClotureBougieCouranteStr']].values[0][0]


                #print("on_bar_update bar sur bougie cloturée")
                print("")
                print(ts, threading.currentThread().getName() , "on_bar_update sur bougie cloturée - ReqId:", reqId, lastTs, Contrat, Periode, "Ts affichage:", TsClotureBougieCouranteStr)
                lastBar = self.cpt.loc[F,'bar'].values[0]
                # print(ts, "  dernière valeur connues : ", lastBar.date, "open:", lastBar.open, "close:",lastBar.close)

                # print(self.cpt)
                
                BarDateDt = datetime.datetime.strptime(bar.date, '%Y%m%d %H:%M:%S')

                self.on_bar_clotured(reqId, self.cpt.loc[F,'bar'].values[0], BarDateDt )


            #Mise à jour de la bougie courante si bougie non cloturée ou 1er update suite à init :

            intPeriode = 15 if Periode=='15min' else 5 if Periode=='5min' else 1
            TsBougieCouranteDt =  datetime.datetime.strptime(bar.date, '%Y%m%d %H:%M:%S')
            TsBougieNextDt     = TsBougieCouranteDt + datetime.timedelta(minutes=intPeriode)
            TsClotureBougieCouranteStr = self.cpt.loc[F,['TsClotureBougieCouranteStr']].values[0][0]

            if not pd.isna(TsClotureBougieCouranteStr) :
                TsClotureBougieCouranteDt = datetime.datetime.strptime(TsClotureBougieCouranteStr, '%Y%m%d %H:%M:%S')
                if TsClotureBougieCouranteDt != TsBougieNextDt:
                    self.cpt.loc[F,['TsClotureBougieCouranteStr']] = TsBougieNextDt.strftime('%Y%m%d %H:%M:%S')
                    print("maj TsClotureBougieCouranteStr:",TsClotureBougieCouranteStr)
    
            if self.cpt.loc[F,'BougieClotured'].values[0] == False :
                self.cpt.loc[F,'TsBougieCouranteStr'] = bar.date
                self.cpt.loc[F,'bar'] = bar
                self.cpt.loc[F,['UpdateStarted']]  = True
                self.cpt.loc[F,['BougieClotured']] = False
                # self.cpt.loc[F,['TsClotureBougieCouranteStr']] = TsBougieNextDt.strftime('%Y%m%d %H:%M:%S')
                
            if self.cpt.loc[F,['UpdateStarted']].values[0] == False:
                self.cpt.loc[F,'TsBougieCouranteStr'] = bar.date
                self.cpt.loc[F,'bar'] = bar
                self.cpt.loc[F,['UpdateStarted']]  = True
                self.cpt.loc[F,['BougieClotured']] = False
                # self.cpt.loc[F,['TsClotureBougieCouranteStr']] = TsBougieNextDt.strftime('%Y%m%d %H:%M:%S')
                print("")
                print("---"*30)
                print(ts, threading.currentThread().getName() , "on_bar_update UpdateStarted - ReqId:", reqId, bar.date, Contrat, Periode)
                print("---"*30)
                print(self.cpt)
        
    



    def on_bar_update_histo(self, reqId, bar):

        # self.Histo_ohlc_day = pd.DataFrame(columns=['Contrat','TsBougieCouranteStr','open','high','low','close','Volume','EMA20','EMA50'])
        # self.Histo_ohlc_15min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50'])
        # self.Histo_ohlc_5min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50'])
        # self.Histo_ohlc_1min = pd.DataFrame(columns=['Contrat','Ts','open','high','low','close','Volume','EMA20','EMA50'])

        ts = datetime.datetime.today()

        # print(ts, threading.currentThread().getName() , "on_bar_update_histo - ReqId:", reqId, ' Bar:', bar)
        
        F=(self.cpt['reqId'] == reqId)
        self.cpt.loc[F,'i']=self.cpt.loc[F,'i']+1
        i_cpt =  self.cpt.loc[F,'i'].values[0]
        self.cpt.loc[F,'TsBougieCouranteStr']=bar.date
        self.cpt.loc[F,'bar']=bar
        #print(i_cpt)

               
        # NQ Day : reqId=2
        if reqId == 2:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d')
            self.i_day=self.i_day+1
            i=self.i_day
            self.Histo_ohlc_day.loc[i] = ['NQ', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, 0, 0, 0, 0, 0, 0, 0, 0, 0]
            
            
        # NQ 15min : reqId=3
        if reqId == 3:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_15min=self.i_15min+1
            i=self.i_15min
            self.Histo_ohlc_15min.loc[i] = ['NQ', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]
            
        # NQ 5min : reqId=4
        if reqId == 4:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_5min=self.i_5min+1
            i=self.i_5min
            self.Histo_ohlc_5min.loc[i] = ['NQ', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]

        # NQ 1min : reqId=5
        if reqId == 5:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_1min=self.i_1min+1
            i=self.i_1min
            self.Histo_ohlc_1min.loc[i] = ['NQ', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]

        # YM Day : reqId=7
        if reqId == 7:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d')
            self.i_day=self.i_day+1
            i=self.i_day
            self.Histo_ohlc_day.loc[i] = ['YM', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]
           
        # YM 15min : reqId=8
        if reqId == 8:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_15min=self.i_15min+1
            i=self.i_15min
            self.Histo_ohlc_15min.loc[i] = ['YM', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]
            
        # YM 5min : reqId=9
        if reqId == 9:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_5min=self.i_5min+1
            i=self.i_5min
            self.Histo_ohlc_5min.loc[i] = ['YM', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]

        # YM 1min : reqId=10
        if reqId == 10:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_1min=self.i_1min+1
            i=self.i_1min
            self.Histo_ohlc_1min.loc[i] = ['YM', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]
        
        # DXM Day : reqId=12
        if reqId == 12:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d')
            self.i_day=self.i_day+1
            i=self.i_day
            self.Histo_ohlc_day.loc[i] = ['DXM', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]
           
        # DXM 15min : reqId=13
        if reqId == 13:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_15min=self.i_15min+1
            i=self.i_15min
            self.Histo_ohlc_15min.loc[i] = ['DXM', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]
            
        # DXM 5min : reqId=14
        if reqId == 14:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_5min=self.i_5min+1
            i=self.i_5min
            self.Histo_ohlc_5min.loc[i] = ['DXM', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]

        # DXM 1min : reqId=15
        if reqId == 15:
            date_ts=datetime.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
            self.i_1min=self.i_1min+1
            i=self.i_1min
            self.Histo_ohlc_1min.loc[i] = ['DXM', date_ts, bar.open, bar.high, bar.low, bar.close, bar.volume, None, None, None, None, None, None, None, None, None]        

        # Bougies Heiken Ashi - Daily
        if reqId in [2, 7, 12]:
            xclose= (bar.open + bar.close + bar.low + bar.high) / 4
            if i_cpt == 1:
                xopen = (bar.open + bar.close) / 2
                xhigh = bar.high
                xlow  = bar.low
            else:
                # print(self.Histo_ohlc_day)
                
                i_cour = i
                i_contrat = self.cpt.loc[F,'Contrat'].values[0]
                F_Contrat = (self.Histo_ohlc_day['Contrat']==i_contrat)
                # print(self.Histo_ohlc_day.loc[F_Contrat].loc[0:i_cour])
                # print(self.Histo_ohlc_day.loc[F_Contrat].loc[0:i_cour].iloc[-2])
                lastXopen = self.Histo_ohlc_day.loc[F_Contrat].loc[0:i_cour].iloc[-2]['xopen']
                # print(lastXopen)
                lastXclose = self.Histo_ohlc_day.loc[F_Contrat].loc[0:i_cour].iloc[-2]['xclose']
                # print(lastXclose)
                
                xopen = (lastXopen + lastXclose ) / 2 
                xhigh = max(max(bar.high, xopen), xclose)
                xlow  = min(min(bar.low, xopen), xclose)
            #print(self.Histo_ohlc_day.loc[i])
            xcouleur='Flat'
            if xclose > xopen + 2:
                xcouleur = 'Green'
            if xclose < xopen - 2:
                xcouleur = 'Red'
            self.Histo_ohlc_day.loc[i,['xopen','xclose','xlow','xhigh','xcouleur']]=[xopen,xclose,xlow,xhigh,xcouleur]

        # Bougies Heiken Ashi - 15min
        if reqId in [3, 8, 13]:
            xclose= (bar.open + bar.close + bar.low + bar.high) / 4
            if i_cpt == 1:
                xopen = bar.open
                xhigh = bar.high
                xlow  = bar.low
            else:
                # xopen = (self.Histo_ohlc_15min.loc[i-1,'xopen'] + self.Histo_ohlc_15min.loc[i-1,'xclose'] ) / 2
                i_cour = i
                i_contrat = self.cpt.loc[F,'Contrat'].values[0]
                F_Contrat = (self.Histo_ohlc_15min['Contrat']==i_contrat)
                lastXopen = self.Histo_ohlc_15min.loc[F_Contrat].loc[0:i_cour].iloc[-2]['xopen']
                lastXclose = self.Histo_ohlc_15min.loc[F_Contrat].loc[0:i_cour].iloc[-2]['xclose']
                xopen = (lastXopen + lastXclose ) / 2
                xhigh = max(max(bar.high, xopen), xclose)
                xlow  = min(min(bar.low, xopen), xclose)
            #print(self.Histo_ohlc_day.loc[i])
            xcouleur='Flat'
            if xclose > xopen + 2:
                xcouleur = 'Green'
            if xclose < xopen - 2:
                xcouleur = 'Red'
            self.Histo_ohlc_15min.loc[i,['xopen','xclose','xlow','xhigh','xcouleur']]=[xopen,xclose,xlow,xhigh,xcouleur]

        # Bougies Heiken Ashi - 5min
        if reqId in [4, 9, 14]:
            xclose= (bar.open + bar.close + bar.low + bar.high) / 4
            if i_cpt == 1:
                xopen = bar.open
                xhigh = bar.high
                xlow  = bar.low
            else:
                # xopen = (self.Histo_ohlc_5min.loc[i-1,'xopen'] + self.Histo_ohlc_5min.loc[i-1,'xclose'] ) / 2
                i_cour = i
                i_contrat = self.cpt.loc[F,'Contrat'].values[0]
                F_Contrat = (self.Histo_ohlc_5min['Contrat']==i_contrat)
                lastXopen = self.Histo_ohlc_5min.loc[F_Contrat].loc[0:i_cour].iloc[-2]['xopen']
                lastXclose = self.Histo_ohlc_5min.loc[F_Contrat].loc[0:i_cour].iloc[-2]['xclose']
                xopen = (lastXopen + lastXclose ) / 2
                xhigh = max(max(bar.high, xopen), xclose)
                xlow  = min(min(bar.low, xopen), xclose)
            #print(self.Histo_ohlc_day.loc[i])
            xcouleur='Flat'
            if xclose > xopen + 2:
                xcouleur = 'Green'
            if xclose < xopen - 2:
                xcouleur = 'Red'
            self.Histo_ohlc_5min.loc[i,['xopen','xclose','xlow','xhigh','xcouleur']]=[xopen,xclose,xlow,xhigh,xcouleur]

        # Bougies Heiken Ashi - 1min
        if reqId in [5, 10, 15]:
            xclose= (bar.open + bar.close + bar.low + bar.high) / 4
            if i_cpt == 1:
                xopen = bar.open
                xhigh = bar.high
                xlow  = bar.low
            else:
                i_cour = i
                i_contrat = self.cpt.loc[F,'Contrat'].values[0]
                F_Contrat = (self.Histo_ohlc_1min['Contrat']==i_contrat)
                lastXopen = self.Histo_ohlc_1min.loc[F_Contrat].loc[0:i_cour].iloc[-2]['xopen']
                lastXclose = self.Histo_ohlc_1min.loc[F_Contrat].loc[0:i_cour].iloc[-2]['xclose']
                xopen = (lastXopen + lastXclose ) / 2
                xhigh = max(max(bar.high, xopen), xclose)
                xlow  = min(min(bar.low, xopen), xclose)
            #print(self.Histo_ohlc_day.loc[i])
            xcouleur='Flat'
            if xclose > xopen + 2:
                xcouleur = 'Green'
            if xclose < xopen - 2:
                xcouleur = 'Red'
            self.Histo_ohlc_1min.loc[i,['xopen','xclose','xlow','xhigh','xcouleur']]=[xopen,xclose,xlow,xhigh,xcouleur]


    def  on_historicalDataEnd(self, reqId):
        
        ts = datetime.datetime.today()         
        self.fin_flux = self.fin_flux+1
        print(ts, "Bot.py - bot.fin_flux:", self.fin_flux)
        
        self.maj_Indicateurs(reqId)
        
        
        if self.fin_flux==12:


            self.cpt['BougieClotured'] = True

            self.maj_TabIndicateurs('init')
            
            for iCpt in self.cpt.index:
                # F_req=(self.cpt['reqId']==iReq)
                iPer=self.cpt.loc[iCpt,['Periode']].values[0]
                print("Periode:",iPer)
                if iPer != '1D':
                    Add_Minutes = 15 if iPer == '15min' else 5 if iPer == '5min' else 1
                    TsBougieCouranteStr = self.cpt.loc[iCpt,['TsBougieCouranteStr']].values[0]
                    print("TsBougieCouranteStr:",TsBougieCouranteStr)
                    TsBougieCouranteDt = datetime.datetime.strptime(TsBougieCouranteStr, '%Y%m%d %H:%M:%S')
                    TsClotureBougieCouranteDt = TsBougieCouranteDt + datetime.timedelta(minutes=2*Add_Minutes) 
                    TsClotureBougieCouranteStr = TsClotureBougieCouranteDt.strftime('%Y%m%d %H:%M:%S')
                    self.cpt.loc[iCpt,['TsClotureBougieCouranteStr']] = TsClotureBougieCouranteStr
                    print("maj TsClotureBougieCouranteStr:",TsClotureBougieCouranteStr,'cpt.index:',iCpt)



            self.cpt['UpdateStarted'] = False

            # F=(self.cpt['Periode'] == '15min')
            # # self.cpt.loc[F,['TsBougieCouranteStr']] = self.cpt.loc[F,['TsBougieCouranteStr']].apply(lambda x: (datetime.datetime.strptime(x.values[0], '%Y%m%d %H:%M:%S') + datetime.timedelta(minutes=15)).strftime('%Y%m%d %H:%M:%S'))
            # self.cpt.loc[F,['BougieClotured']] = False
            # F=(self.cpt['Periode'] == '5min')
            # # self.cpt.loc[F,['TsBougieCouranteStr']] = self.cpt.loc[F,['TsBougieCouranteStr']] + datetime.timedelta(minutes=5)
            # self.cpt.loc[F,['BougieClotured']] = False
            # F=(self.cpt['Periode'] == '1min')
            # # self.cpt.loc[F,['TsBougieCouranteStr']] = self.cpt.loc[F,['TsBougieCouranteStr']] + datetime.timedelta(minutes=1)
            # self.cpt.loc[F,['BougieClotured']] = False
             
            print(self.cpt)

    def  maj_Indicateurs(self, reqId):

        ts = datetime.datetime.today()         
        print(ts, threading.currentThread().getName() , "maj_Indicateurs - ReqId:", reqId)


        F=(self.cpt['reqId'] == reqId)
        iContrat = self.cpt.loc[F,'Contrat'].values[0]
        iPeriode = self.cpt.loc[F,'Periode'].values[0]
        print("maj_Indicateurs ", iContrat, iPeriode)
        
        if iPeriode == "1D":

            #print(self.Histo_ohlc_day)
            F=(self.Histo_ohlc_day['Contrat']==iContrat)
            df0= self.Histo_ohlc_day.loc[F]  
            
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


        ts = datetime.datetime.today()         
        # print("")
        # print("---"*30)
        # print(ts, "maj_TabIndicateurs opt: ", opt)
        # print("---"*30)
        # print(self.cpt)
       
        
        def RechercheFreinsTendance(Contrat, TailleBougies, MinMaxDf):
            # Cette proc recherche dans les Niveaux une explication  au retournement de tendance 
            # Param d'entrée :
            #   - Contrat analysé
            #   - TailleBougies : si bougies Q, on ne prend pas en compte les niveaux du Jour
            #   - 1 dataframe MinMaxDf qui contient la liste des pics  analyser. Ce dataframe contient :
            #     - type : Min ou Max
            #     - Ts : heure du pic
            #     - Val : Prix du pic
            #     - NbPeriodes : durée de la tendance qui prend fin sur ce pic, exprimée en nb de périodes
            #     - EcartMinMax : Amplitude de la tendance qui prend fin
            #     - NbCulsPlats : Nb de bougies HA "cul plat" durant cette tenadance : indicateur de la force de la tendance
            #     - NbMechesRejets : Nb de bougies d'hésitation durant cette tendance : indicateur de faiblesse de la tendance
            #     - EMA20 : au moment du pic
            #     - EMA50 : au moment du pic
            #     - xclose : cloture HA de la bougie du pic. Par exemple, pour un max, on estime qu'une Resistance expliquant le retournement 
            #                peut être un niveau contre lequel vient buter le pic ou le xclose
            # En sortie le df self.ListeFreins ui ne contient que la colonne self.ListeFreins qui contient la liste des niveaux qui sont responsables du retournement
            MinMaxDf_ListeFreins = MinMaxDf.copy()
            MinMaxDf_ListeFreins['ListeFreins']='Init1'

            # print("---"*40)
            # print("RechercheFreinsTendance : MinMaxDf :", Contrat, TailleBougies)
            # print(MinMaxDf)
            # print("---"*40)
            # print("RechercheFreinsTendance : MinMaxDf First 3:", Contrat, TailleBougies)
            # print(MinMaxDf[:3])
            
            Niv = self.NiveauxSupJ if TailleBougies == '1D' else self.NiveauxJ
            F = (Niv['Contrat']==Contrat)
            iNiv = Niv.loc[F,['Niveau','Prix']].copy()
            iNiv['Typo'] = 'Nivo'
           
            cpt=0 
            for i in MinMaxDf.index[:10]:
                TypePic = MinMaxDf.Type[i]
                PrixPic = MinMaxDf.Val[i]
                TsPic   = MinMaxDf.Ts[i]
                EMA20   = MinMaxDf.EMA20[i] 
                EMA50   = MinMaxDf.EMA50[i] 
                xclose  = MinMaxDf.xclose[i] 
           
                iNiv = iNiv.append({"Niveau":"EMA20", "Prix":EMA20,"Typo":str(cpt) }, ignore_index=True)
                iNiv = iNiv.append({"Niveau":"EMA50", "Prix":EMA50,"Typo":str(cpt) }, ignore_index=True)
                # iNiv = iNiv.append({"Niveau":str(cpt)+' - ' +TypePic+'   ----',  "Prix":PrixPic,"Typo":str(cpt)}, ignore_index=True)
                # iNiv = iNiv.append({"Niveau":str(cpt)+" - xclose"+'----',  "Prix":xclose,"Typo":str(cpt)}, ignore_index=True)
                
                # Recherche des R/S qui sont entrés en jeu :
                iNiv['EcartAbs'] = iNiv['Prix'].apply(lambda x : abs(x - PrixPic))
                iNiv.sort_values(by=['EcartAbs'], ascending=True, inplace=True)
                
                cptStr=str(cpt)
                
                F1=(iNiv["Typo"] == cptStr) | (iNiv["Typo"] == 'Nivo')
                F2=(iNiv["EcartAbs"] < 50)
                iNiv_Pic = iNiv.loc[F1 & F2].copy()
                iNiv_Pic['Close_Pic'] = 'Pic'
                iNiv_Pic['Close_Pic_Prix'] = PrixPic
                
                # print("---"*40)
                # print("RechercheFreinsTendance : EcartsAbs vs Pic:", Contrat, TailleBougies)
                # print(iNiv[:10])
                # print("---"*10)
                # print(iNiv_Pic)
                
                iNiv['EcartAbs'] = iNiv['Prix'].apply(lambda x : abs(x - xclose))
                iNiv.sort_values(by=['EcartAbs'], ascending=True, inplace=True)

                F1=(iNiv["Typo"] == cptStr) | (iNiv["Typo"] == 'Nivo')
                F2=(iNiv["EcartAbs"] < 50)
                iNiv_xclose = iNiv.loc[F1 & F2].copy()
                iNiv_xclose['Close_Pic'] = 'xclose'
                iNiv_xclose['Close_Pic_Prix'] = xclose

                # print("---"*40)
                # print("RechercheFreinsTendance : EcartsAbs vs xclose:", Contrat, TailleBougies)
                # print(iNiv[:10])
                # print("---"*10)
                # print(iNiv_xclose)
                
                iRes = pd.concat([iNiv_Pic, iNiv_xclose])

                # print("---"*40)
                # print("RechercheFreinsTendance : EcartsAbs vs xclose & pic:", Contrat, TailleBougies, " - ",TypePic," de ", TsPic, " à ", PrixPic, "en cours depuis ",MinMaxDf.NbPeriodes[i], "périodes, d'amplitude ", MinMaxDf.EcartMinMax[i],"points")
                cols_iRes = ['Close_Pic','Close_Pic_Prix','Niveau','Prix','EcartAbs']
                iRes2 = iRes[cols_iRes].copy()
                iRes2.sort_values(by=['EcartAbs'], ascending=True, inplace=True)
                F3=(iRes2["EcartAbs"] < 20)
                #len(iRes.loc[F3])
                NbLignes = max(1,len(iRes2.loc[F3]))
                # print(iRes2)
                # print(iRes2[:NbLignes])
                
                    
                
                # MinMaxDf.Frein[i] = 
                # print("Liste des FREINS :")
                iListeFreins = iRes2[:NbLignes].sort_values(['Niveau']).drop_duplicates(subset=['Niveau'], keep='last').Niveau.to_list()
                # print(iListeFreins)
                # self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'self.ListeFreins':iListeFreins}, ignore_index=True)
                # MinMaxDf_ListeFreins.at[i,['self.ListeFreins']]='Init2'
                MinMaxDf_ListeFreins.at[i,'ListeFreins']=iListeFreins
                
                # MinMaxDf.loc[i,['self.ListeFreins']] = 'Init2'
                # MinMaxDf.loc[i,['self.ListeFreins']] = self.ListeFreins
                
                cpt=cpt+1
            
            # iNiv.sort_values(by=['Prix'], ascending=False, inplace=True)
            # print("---"*40)
            # print("---"*40)
            # print("RechercheFreinsTendance : self.ListeFreins :", Contrat, TailleBougies)
            # # print(MinMaxDf)
            # print(MinMaxDf_ListeFreins)
            return(MinMaxDf_ListeFreins)
            
            
                
            
            

        def MinMax(df100, Contrat, TailleBougies, opt, NbARendre):
            # df100 : dataframe avec au moins Ts, high, low, xcouleur, xopen/xclose/xlow/xhigh, EMA20, EMA50 et 100 periodes d'historique
            # Contrat : contrat cojncerné par df100
            # TailleBougies : taille des bougies du df100 (1D, 15min, etc)
            # opt : df : return un dataframe / serie : return une liste [] / df+serie : return df + liste
            # NbARendre :  nb de [pics] à restituer. Si 5 : on rend les 5 derniers pics
            # Pour chaque pic, les infos suivantes sont restituées :
            #   - Type pic (Min / Max)    
            #   - TS du pic     
            #   - Prix du pic     
            #   - Nombre de périodes écoulées depuis dernier pic ( = durée de la tendance qui prend fin)
            #   - Variation enregistrées depuis le dernier pic ( = variation enregistrée durant la tendance qui prend fin)     
            #   - Nombre de bougies "Cul-plat" ( = indicateur de la force de la tendance qui prend fin)     
            #   - Nombre de mèches de rejets ( = indicateur de faiblesse de la tendance qui prend fin)   
            
            
            # print("****"*30)
            # print(df100)
            df=df100.reset_index().copy()
            # print("****"*30 + "Procedure MinMax")
            # print(df)
            lastMaj = ""
            Min = df.low[0]
            TsMin = df.Ts[0]
            Max = df.high[0]
            TsMax = df.Ts[0]
            MinMaxDf    = pd.DataFrame()
            MinMaxSerie = []
            NbPer = 0
            NbCulsPlats = 0
            NbMechesRejets = 0

            # print("MinMax 02")
            # tm.sleep(1)
                
            # print(Min)
            # print(Max)
            ts = datetime.datetime.today()
            # print(ts,  "DEB...  " )
            iMax = 1
            iMin = 1
            for i in range(1, len(df)-1):
                xopen  = df.xopen[i]
                xclose = df.xclose[i]
                xlow   = df.xlow[i]
                xhigh  = df.xhigh[i]
                # EMA20  = df.EMA20[i]
                # EMA50  = df.EMA50[i]
                if df.xcouleur[i] == "Green":
                    high = df.high[i]
                    if lastMaj != "Max":
                        xclose_Min = df.xclose[iMin]
                        EMA20_Min  = df.EMA20[iMin]
                        EMA50_Min  = df.EMA50[iMin]
                        
                        Ecart='{:0.1f}'.format(Min-Max)
                        MinMaxDf = MinMaxDf.append({"Type":"Min", "Ts":TsMin,"Val": Min, "NbPeriodes": NbPer, "EcartMinMax":Ecart,"NbCulsPlats":NbCulsPlats ,"NbMechesRejets":NbMechesRejets, 'EMA20':EMA20_Min,'EMA50':EMA50_Min, 'xclose':xclose_Min}, ignore_index=True)
                        MinMaxSerie = MinMaxSerie + [["Min", TsMin.strftime("%Y-%m-%d %H:%M:%S"), Min, NbPer, Ecart, NbCulsPlats, NbMechesRejets]]
                        # RechercheFreinsTendance(Contrat, 'Min', Min, xclose, Max, TailleBougies, EMA20, EMA50)

                        Max = Min
                        NbPer = 1
                        NbCulsPlats = 0
                        NbMechesRejets = 0
                        #TsMax = df.Ts[i]
                    if df.high[i] > Max:
                        Max = high
                        TsMax = df.Ts[i]
                        iMax = i
                        lastMaj = "Max"

                    NbPer = NbPer + 1
                    if xlow >= xopen: 
                        NbCulsPlats = NbCulsPlats + 1
                    if xhigh-xclose > xclose-xopen:
                        NbMechesRejets = NbMechesRejets + 1
                        
                        
                if df.xcouleur[i] == "Red":
                    low = df.low[i]
                    if lastMaj != "Min":
                        xclose_Max = df.xclose[iMax]
                        EMA20_Max  = df.EMA20[iMax]
                        EMA50_Max  = df.EMA50[iMax]

                        Ecart='{:0.1f}'.format(Max-Min)
                        MinMaxDf = MinMaxDf.append({"Type":"Max", "Ts":TsMax,"Val": Max, "NbPeriodes": NbPer, "EcartMinMax":Ecart,"NbCulsPlats":NbCulsPlats,"NbMechesRejets":NbMechesRejets, 'EMA20':EMA20_Max,'EMA50':EMA50_Max, 'xclose':xclose_Max}, ignore_index=True)
                        MinMaxSerie = MinMaxSerie + [["Max", TsMax.strftime("%Y-%m-%d %H:%M:%S"), Max, NbPer, Ecart,NbCulsPlats, NbMechesRejets]]
                        # RechercheFreinsTendance(Contrat, 'Max', Max, xclose, Min, TailleBougies, EMA20, EMA50)
                        Min = Max
                        NbPer = 1
                        NbCulsPlats = 0
                        NbMechesRejets = 0
                        #TsMin = df.Ts[i]
                    if df.low[i] < Min:
                        Min = low
                        TsMin = df.Ts[i]
                        iMin = i
                        lastMaj = "Min"

                    NbPer = NbPer + 1
                    if xhigh <= xopen: 
                        NbCulsPlats = NbCulsPlats + 1
                    if xclose-xlow > xopen-xclose:
                        NbMechesRejets = NbMechesRejets + 1

            # #Ajout de la valeur courante :
            # i = len(df)-1
            # # print("TEST STEST df[i]")
            # # print(df.Ts[i])
            # # print("TEST STEST df[-1]")
            # # print(df.iloc[-1])
            # Ecart = '{:0.1f}'.format(df.high[i]-Min) if  lastMaj == "Min"  else  '{:0.1f}'.format(df.low[i]-Max) 
            # Val   =  '{:0.1f}'.format(df.high[i]) if  lastMaj == "Min"  else  '{:0.1f}'.format(df.low[i]) 
            # MinMaxDf = MinMaxDf.append({"Type":"Cour", "Ts":df.Ts[i],"Val": Val, "NbPeriodes": NbPer, "EcartMinMax":Ecart,"NbCulsPlats":NbCulsPlats,"NbMechesRejets":NbMechesRejets, 'EMA20':df.EMA20[i],'EMA50':df.EMA50[i], 'xclose':df.xclose[i]}, ignore_index=True)
            
            # print("MinMax 03")
            # tm.sleep(1)
            

            # ts = datetime.datetime.today()
            cols=["Type", "Ts","Val", "NbPeriodes", "EcartMinMax","NbCulsPlats" ,"NbMechesRejets","EMA20","EMA50","xclose","ListeFreins"]
            MinMaxDf2 = MinMaxDf.iloc[-NbARendre:].sort_values(by=['Ts'], ascending=False).copy()   
            MinMaxDf2['ListeFreins']='Init'

            # print("---"*40)
            # print("MinMax - Liste des 5 derniers pic (MinMax avec causes des freins) :", Contrat, TailleBougies )
            # print(MinMaxDf2[cols])

            MinMaxDf3 = RechercheFreinsTendance(Contrat, TailleBougies, MinMaxDf2[cols])
            MinMaxDf3.reset_index(inplace=True)
            MinMaxDf3.drop(columns=['index'],inplace=True)   

            # print("---"*40)
            # print("MinMax - Liste des 5 derniers pic (MinMax avec causes des freins) :", Contrat, TailleBougies )
            # print(MinMaxDf3[cols])
            # print(ts,  "FIN...  " )        
            #MinMax.to_csv("C:\Temp\MinMax.csv",sep=';',decimal='.',float_format='%.1f', index=False)
            if opt == "df":
                return(MinMaxDf3)
            if opt == "serie":
                res = [pic for pic in reversed(MinMaxSerie[-NbARendre:])]
                return(res)
            if opt == "df+serie":
                res = [pic for pic in reversed(MinMaxSerie[-NbARendre:])]
                return(MinMaxDf3, res)
            
    

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
            self.NiveauxSupJ = pd.DataFrame()  # Niveaux sans les niveaux Q (pivots, close,etc), pas assez forts pour les bougies Q
            self.NiveauxSupJ = None
            self.TabSuiviIndicateurs = pd.DataFrame(columns=['Contrat','ts','Prix','Mvt','1D','15min','5min','1min'])
            self.TabSuiviKPI = pd.DataFrame()

            
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
            
            # Sélection des niveaux suffisamment forts pour les bougies Q :
            F_notJ = (self.NiveauxJ['Niveau'].apply(lambda x : x[-1]) != 'J')
            self.NiveauxSupJ = self.NiveauxJ.loc[F_notJ].copy()
            
            
            # print("***"*20)
            # print(" self.NiveauxSupJ :")
            # print(self.NiveauxSupJ)            
            

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
            # print(iContrat)

            
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
                Ts1minCourant= lastLigne['Ts']


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
        
            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]

                # print("lastLigne")
                # print(lastLigne)

                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['EMA20'] > lastLigne['EMA50']

            if opt in ['init','15min','5min']:
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

            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['close'] > lastLigne['EMA50']

            if opt in ['init','15min','5min']:
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

            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-6]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 10000*( lastLigne['EMA50'] - ligneRef['EMA50'] ) / ligneRef['EMA50']

            if opt in ['init','15min','5min']:
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

            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['RSI14'] > 50

            if opt in ['init','15min','5min']:
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

            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 10000*( lastLigne['EMA05'] - ligneRef['EMA05'] ) / ligneRef['EMA05']

            if opt in ['init','15min','5min']:
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

            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['xcouleur'] 

            if opt in ['init','15min','5min']:
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
            
            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                F2 = (df['xcouleur'] != lastLigne['xcouleur'] )
                NB1=df.index[-1]
                NB2=df.loc[F2].index[-1]
                NB3=len(df.loc[NB2:NB1])-1
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = NB3

            if opt in ['init','15min','5min']:
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
                # print(df.loc[NB2:NB1])
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
                    if self.TabIndicateurs.loc[(ts_i, iContrat, 'Nb_Bougies_meme_Couleur'),col] >= 1 :
                        Tendance_CT_UP = Tendance_CT_UP + 1
                elif self.TabIndicateurs.loc[(ts_i, iContrat, 'Couleur_Last_Bougie_HA'),col] == 'Red' :
                    Tendance_CT_DOWN = Tendance_CT_DOWN + 1
                    if self.TabIndicateurs.loc[(ts_i, iContrat, 'Nb_Bougies_meme_Couleur'),col] >= 1 :
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
            
            self.ListeFreins = pd.DataFrame()

            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['close'] - lastLigne['EMA20']
                self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '1D' , 'NomFrein':'EMA20', 
                              'ValFrein':lastLigne['EMA20'], 'DistFrein':lastLigne['close'] - lastLigne['EMA20']}, ignore_index=True)


            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['close'] - lastLigne['EMA20']
                self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '15min' , 'NomFrein':'EMA20', 
                              'ValFrein':lastLigne['EMA20'], 'DistFrein':lastLigne['close'] - lastLigne['EMA20']}, ignore_index=True)

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['close'] - lastLigne['EMA20']
                self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '5min' , 'NomFrein':'EMA20', 
                              'ValFrein':lastLigne['EMA20'], 'DistFrein':lastLigne['close'] - lastLigne['EMA20']}, ignore_index=True)

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['close'] - lastLigne['EMA20']
                self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '1min' , 'NomFrein':'EMA20', 
                              'ValFrein':lastLigne['EMA20'], 'DistFrein':lastLigne['close'] - lastLigne['EMA20']}, ignore_index=True)




            KPI='Dist_EMA50'
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = lastLigne['close'] - lastLigne['EMA50']
                self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '1D' , 'NomFrein':'EMA50', 
                              'ValFrein':lastLigne['EMA50'], 'DistFrein':lastLigne['close'] - lastLigne['EMA50']}, ignore_index=True)

            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = lastLigne['close'] - lastLigne['EMA50']
                self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '15min' , 'NomFrein':'EMA50', 
                              'ValFrein':lastLigne['EMA50'], 'DistFrein':lastLigne['close'] - lastLigne['EMA50']}, ignore_index=True)

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = lastLigne['close'] - lastLigne['EMA50']
                self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '5min' , 'NomFrein':'EMA50', 
                              'ValFrein':lastLigne['EMA50'], 'DistFrein':lastLigne['close'] - lastLigne['EMA50']}, ignore_index=True)

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                ligneRef=df.iloc[-2]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = lastLigne['close'] - lastLigne['EMA50']
                self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '1min' , 'NomFrein':'EMA50', 
                              'ValFrein':lastLigne['EMA50'], 'DistFrein':lastLigne['close'] - lastLigne['EMA50']}, ignore_index=True)
                

            KPI='Dist_Niveaux'  
            
            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F]
                lastLigne=df.iloc[-1]
                lastPrix = lastLigne['close']
                # print("lastPrix:",lastPrix)
                F2 = (self.NiveauxSupJ['Contrat'] == iContrat)
                iNiv= self.NiveauxSupJ.loc[F2]

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

                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = 'init'
                    # print('cellule : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D']) )
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = []
                    # print('cellule : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D']) )
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = ['Niv']
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = ['Na','b']
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = ['Na','b','c']
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = ['Niv',0,0]
                    # print("Rr_desc:",Rr_desc, 'type : ', type(Rr_desc))
                    try:
                        self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = str(Rr_desc)
                    except Exception as e:
                        print(e)
                        print("ts_i : " , ts_i)
                        print("iContrat : ", iContrat)
                        print("KPI : " + KPI)
                        print("Rr_desc:",Rr_desc, 'type : ', type(Rr_desc))
                        print('cellule : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D']) )
                        print(self.TabIndicateurs)

                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] =  ['a','b','c']
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = Rr_desc
                    self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '1D' , 'NomFrein':Rr['Niveau'], 
                              'ValFrein':Rr['Prix'], 'DistFrein':  lastPrix - Rr['Prix']}, ignore_index=True)

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
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] =  'Init'
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = []
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = str(Ss_desc)
                    self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '1D' , 'NomFrein':Ss['Niveau'], 
                              'ValFrein':Ss['Prix'], 'DistFrein':lastPrix - Ss['Prix']}, ignore_index=True)

            if opt in ['init','15min']:
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
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 'init'
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = []
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = ['Niv']
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = ['a',0,0]
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = str(Rr_desc)
                    self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '15min' , 'NomFrein':Rr['Niveau'], 
                              'ValFrein':Rr['Prix'], 'DistFrein':  lastPrix - Rr['Prix']}, ignore_index=True)

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
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 'init'
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = []
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = str(Ss_desc)
                    self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '15min' , 'NomFrein':Ss['Niveau'], 
                              'ValFrein':Ss['Prix'], 'DistFrein':lastPrix - Ss['Prix']}, ignore_index=True)

            if opt in ['init','15min','5min']:
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
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = 'init'
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = []
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = str(Rr_desc)
                    self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '5min' , 'NomFrein':Rr['Niveau'], 
                              'ValFrein':Rr['Prix'], 'DistFrein':  lastPrix - Rr['Prix']}, ignore_index=True)

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
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = 'init'
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = []
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = str(Ss_desc)
                    self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '5min' , 'NomFrein':Ss['Niveau'], 
                              'ValFrein':Ss['Prix'], 'DistFrein':lastPrix - Ss['Prix']}, ignore_index=True)

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F]
                lastLigne=df.iloc[-1]
                beforeLastLigne = df.iloc[-2]
                lastPrix = lastLigne['close']
                beforeLastPrix = beforeLastLigne['close']
                Mvt = lastPrix - beforeLastPrix
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
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = 'init'
                    # print('cellule : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min']) )

                    try:
                        # print('-'*100)
                        # print('ts_i:',ts_i,' iContrat:',iContrat,' KPI:',KPI ,' 1min' )
                        # print('cellule : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min']) )
                        # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = ['a']
                        # print('Init cellule en liste OK : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min']) )
                        # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = ['a','b','c']
                        # print('Init cellule en liste OK : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min']) )
                        self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = str(Rr_desc)
                        # print('Maj cellule  OK : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min']) )
                    except Exception as e:
                        print(e)
                        print("ts_i : " , ts_i)
                        print("iContrat : ", iContrat)
                        print("KPI : " + KPI)
                        print("Rr_desc:",Rr_desc, 'type : ', type(Rr_desc))
                        print('cellule : ' , self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'], 'type : ', type(self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min']) )
                        print(self.TabIndicateurs)
                        self.TabIndicateurs.to_csv("C:\\Temp\\TabIndicateurs.csv",sep=';',decimal='.',float_format='%.1f', index=True)
                    
                    self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '1min' , 'NomFrein':Rr['Niveau'], 
                              'ValFrein':Rr['Prix'], 'DistFrein':  lastPrix - Rr['Prix']}, ignore_index=True)

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
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = 'init'
                    # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = []
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = str(Ss_desc)
                    self.ListeFreins = self.ListeFreins.append({'Ts' : ts_i,'Contrat':iContrat, 'TailleBougie': '1min' , 'NomFrein':Ss['Niveau'], 
                              'ValFrein':Ss['Prix'], 'DistFrein':lastPrix - Ss['Prix']}, ignore_index=True)



            sc1day =  self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),'1D']
            sc15min =  self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),'15min']
            sc5min =  self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),'5min']
            sc1min =  self.TabIndicateurs.loc[(ts_i, iContrat, 'Scénario_Tendance'),'1min']


            KPI='Cible'  

            # pec             KPI='Dist_EMA50' KPI='Dist_EMA20'
            # pec             KPI='Dist_Niveaux'    
            
            if opt != 'init':
                # print("***"*30)
                # print("KPI='Cible'")
                # print(self.ListeFreins)
                F_ts = (self.ListeFreins['Ts']==ts_i)
                F_ct = (self.ListeFreins['Contrat'] == iContrat)
                F_tb = (self.ListeFreins['TailleBougie'] == opt)
                ListeFreinsTsi = self.ListeFreins.loc[F_ts & F_ct & F_tb].copy()
                ListeFreinsTsi.sort_values(by=['DistFrein'], ascending=True, inplace=True)
                # print("***"*30)
                # print("KPI='Cible'")
                # print(ListeFreinsTsi)
                # firstLigne = ListeFreinsTsi.iloc[0]
                
                xcouleur = self.TabIndicateurs.loc[(ts_i, iContrat, 'Couleur_Last_Bougie_HA'),opt]
                self.TabIndicateurs.loc[(ts_i, iContrat, KPI),opt] = 'Flat'
                if xcouleur == 'Green':
                    F_posneg = (ListeFreinsTsi['DistFrein'] < 0)
                    df=ListeFreinsTsi.loc[F_posneg].copy()
                    BonneLigne = df.iloc[-1]
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),opt] = BonneLigne['NomFrein']
                if xcouleur == 'Red':
                    F_posneg = (ListeFreinsTsi['DistFrein'] > 0)
                    df=ListeFreinsTsi.loc[F_posneg].copy()
                    BonneLigne = df.iloc[0]
                    self.TabIndicateurs.loc[(ts_i, iContrat, KPI),opt] = BonneLigne['NomFrein']
                

            


            KPI='MinMax100per'  



            if opt in ['init','1D']:
                F=(self.Histo_ohlc_day['Contrat'] == iContrat)
                df = self.Histo_ohlc_day.loc[F].copy()
                lastLigne=df.iloc[-1]
                DateDeb100per = lastLigne['Date'] + datetime.timedelta(days=-100)
                F_100per = (df['Date'] >= DateDeb100per)
                df100 = df.loc[F_100per,['Date','high','low','xcouleur','xopen','xclose','xlow','xhigh','EMA20','EMA50']].copy()
                df100.rename(columns={ 'Date': 'Ts'}, inplace=True)

                # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = 'Init'
                MinMaxDf, MinMaxSerie = MinMax(df100,iContrat,'1D','df+serie',15)
                # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1D'] = MinMaxSerie
                
                # print("KPI = MinMax100per " + iContrat)
                # print(MinMaxDf)
                # self.TabIndicateurs.loc[(ts_i, iContrat, 'LastPic_Type'),'1D'] = MinMaxDf.Type[0]
                # self.TabIndicateurs.loc[(ts_i, iContrat, 'LastPic_Ts'),'1D'] = MinMaxDf.Ts[0]
                # self.TabIndicateurs.loc[(ts_i, iContrat, 'LastPic_Type'),'1D'] = MinMaxDf.Type[0]
                # self.TabIndicateurs.loc[(ts_i, iContrat, 'LastPic_Type'),'1D'] = MinMaxDf.Type[0]
                
                MinMaxDf['Contrat']=iContrat
                MinMaxDf['TailleBougie']='1D'
                cols = MinMaxDf.columns.tolist()
                cols = cols[-2:] + cols[:-2]
                self.HistoMinMaxDf = pd.concat([self.HistoMinMaxDf,MinMaxDf[cols]])
        
            if opt in ['init','15min']:
                F=(self.Histo_ohlc_15min['Contrat'] == iContrat)
                df = self.Histo_ohlc_15min.loc[F].copy()
                lastLigne=df.iloc[-1]
                DateDeb100per = lastLigne['Ts'] + datetime.timedelta(minutes=-100*15)
                F_100per = (df['Ts'] >= DateDeb100per)
                df100 = df.loc[F_100per,['Ts','high','low','xcouleur','xopen','xclose','xlow','xhigh','EMA20','EMA50']].copy()

                # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min'] = 'Init'
                MinMaxDf, MinMaxSerie = MinMax(df100,iContrat,'15min','df+serie',15)
                # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'15min']  = MinMaxSerie

                MinMaxDf['Contrat']=iContrat
                MinMaxDf['TailleBougie']='15min'
                cols = MinMaxDf.columns.tolist()
                cols = cols[-2:] + cols[:-2]
                self.HistoMinMaxDf = pd.concat([self.HistoMinMaxDf,MinMaxDf[cols]])
                # print("KPI = MinMax100per")
                # print(MinMaxDf)

            if opt in ['init','15min','5min']:
                F=(self.Histo_ohlc_5min['Contrat'] == iContrat)
                df = self.Histo_ohlc_5min.loc[F].copy()
                lastLigne=df.iloc[-1]
                DateDeb100per = lastLigne['Ts'] + datetime.timedelta(minutes=-100*5)
                F_100per = (df['Ts'] >= DateDeb100per)
                df100 = df.loc[F_100per,['Ts','high','low','xcouleur','xopen','xclose','xlow','xhigh','EMA20','EMA50']].copy()

                # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = 'Init'
                MinMaxDf, MinMaxSerie = MinMax(df100,iContrat,'5min','df+serie',15)
                # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'5min'] = MinMaxSerie

                MinMaxDf['Contrat']=iContrat
                MinMaxDf['TailleBougie']='5min'
                cols = MinMaxDf.columns.tolist()
                cols = cols[-2:] + cols[:-2]
                self.HistoMinMaxDf = pd.concat([self.HistoMinMaxDf,MinMaxDf[cols]])
                # print("KPI = MinMax100per")
                # print(MinMaxDf)

            if opt in ['init','15min','5min','1min']:
                F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
                df = self.Histo_ohlc_1min.loc[F].copy()
                lastLigne=df.iloc[-1]
                DateDeb100per = lastLigne['Ts'] + datetime.timedelta(minutes=-100*1)
                F_100per = (df['Ts'] >= DateDeb100per)
                df100 = df.loc[F_100per,['Ts','high','low','xcouleur','xopen','xclose','xlow','xhigh','EMA20','EMA50']].copy()

                # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = 'Init'
                MinMaxDf, MinMaxSerie = MinMax(df100,iContrat,'1min','df+serie',15)
                # self.TabIndicateurs.loc[(ts_i, iContrat, KPI),'1min'] = MinMaxSerie

                MinMaxDf['Contrat']=iContrat
                MinMaxDf['TailleBougie']='1min'
                cols = MinMaxDf.columns.tolist()
                cols = cols[-2:] + cols[:-2]
                self.HistoMinMaxDf = pd.concat([self.HistoMinMaxDf,MinMaxDf[cols]])
                # print("KPI = MinMax100per")
                # print(MinMaxDf)
            


            # drop duplicates de HistoMinMaxDf :
            TailleBougie_order = CategoricalDtype(['1D', '15min', '5min', '1min'], ordered=True)
            self.HistoMinMaxDf['TailleBougie'] = self.HistoMinMaxDf['TailleBougie'].astype(TailleBougie_order) 
            self.HistoMinMaxDf = self.HistoMinMaxDf.sort_values(['Contrat','TailleBougie','Ts'], ascending=False).drop_duplicates(subset=['Contrat','TailleBougie','Ts'], keep='first')
            # print("***"*30)          
            # print("self.HistoMinMaxDf")
            # print(self.HistoMinMaxDf)
                
            
            # print("maj_TabIndicateurs 20")

            # print(self.Histo_ohlc_5min)
            # if opt in ['init','1min']:
            #     F=(self.Histo_ohlc_1min['Contrat'] == iContrat)
            #     df = self.Histo_ohlc_1min.loc[F]
            #     print(df)
            #     nbl = len(df)
            #     nbPer=min(100,nbl)
            #     lastLigne  = df.iloc[-1]
            #     firstLigne = df.iloc[-1-nbPer]
            #     for i in df.iloc[-nbPer:]:
                    
                    
                


            self.TabSuiviIndicateurs = self.TabSuiviIndicateurs.append({'Contrat':iContrat,'ts':Ts1minCourant,'Prix': lastPrix, 'Mvt':Mvt,
                                                                         '1D':sc1day,'15min':sc15min,'5min':sc5min,'1min':sc1min}, ignore_index=True)
            self.TabSuiviIndicateurs = self.TabSuiviIndicateurs.sort_values(['Contrat','ts']).drop_duplicates(subset=['Contrat','ts'], keep='last')

            # print("maj_TabIndicateurs 21")
           

        #self.TabHistoIndicateurs = pd.concat([self.TabHistoIndicateurs, self.TabIndicateurs])
        ts_str = datetime.datetime.today()  


        
        # print('==='*50)
        # print('   '*10,' TabIndicateurs: ',ts_str)
        # print('==='*50)
        # print(self.TabIndicateurs)
        # print("")
        # print('TabSuiviIndicateurs:')
        self.TabSuiviIndicateurs.sort_values(by=['Contrat','ts'], ascending=True, inplace=True)
        # print(self.TabSuiviIndicateurs)
        # print("maj_TabIndicateurs 22")
        # tm.sleep(1)
        self.maj_TabSuiviKPI()
        # print("maj_TabIndicateurs 23")
        self.getAndStoreKPI()
        




        
    def maj_TabSuiviKPI(self):
        
        ts = datetime.datetime.today()  
        # print(ts, threading.currentThread().getName() , "maj_TabSuiviKPI")

        self.TabIndicateurs.reset_index(inplace=True)
        # print(self.TabIndicateurs)

        for iContrat in self.Histo_ohlc_day['Contrat'].unique():
            # print(iContrat)
            
            F=(self.TabIndicateurs['Contrat'] == iContrat)
            df = self.TabIndicateurs.loc[F].copy()
            df.drop(columns=['ts','Contrat'],inplace=True)  
            # print(df)
            
            F_KPI = F=(df['KPI'] == "LAST_BOUGIE")
            tsStr = df.loc[F_KPI,'1min'].values[0]
            # print("maj_TabSuiviKPI 01")
            for c in df.columns[1:]:
                c_df = df[['KPI',c]]
                c_df_T = c_df.T
                cols=c_df_T.iloc[0,0:].tolist()
                c_df_T.columns = cols
                c_df_T.drop(['KPI'],inplace=True)
                c_df_T.reset_index(inplace=True)
                c_df_T.drop(columns=['index'],inplace=True)  
                c_df_T['ts']=tsStr
                c_df_T['Contrat']=iContrat
                c_df_T['Periode']=c
                c2=c_df_T.columns.tolist()
                cols2 = ['ts','Contrat','Periode'] + c2[:-3]
                d=c_df_T[cols2]
                # print("d_complet:")                
                # print(d)
                # print("---"*20)
                self.TabSuiviKPI = pd.concat([self.TabSuiviKPI,d])

        # print("maj_TabSuiviKPI 02")

        Periode_order = CategoricalDtype(['1D', '15min', '5min', '1min'], ordered=True)
        self.TabSuiviKPI['Periode'] = self.TabSuiviKPI['Periode'].astype(Periode_order) 
        self.TabSuiviKPI = self.TabSuiviKPI.sort_values(['ts','Contrat','Periode']).drop_duplicates(subset=['ts','Contrat','Periode'], keep='last')
        self.TabSuiviKPI[['Pente_EMA50_5','Pente_EMA05_2','Dist_EMA20','Dist_EMA50']] = self.TabSuiviKPI[['Pente_EMA50_5','Pente_EMA05_2','Dist_EMA20','Dist_EMA50']].astype(float)
        
        # print("maj_TabSuiviKPI 03")
        
        self.TabSuiviKPI.to_csv("C:\\Temp\\tabsuiviKPI.csv",sep=';',decimal='.',float_format='%.1f', index=False)
        self.TabIndicateurs.set_index(['ts','Contrat','KPI'], inplace=True)

        # print("maj_TabSuiviKPI:")
        # print(self.TabSuiviKPI)


    def getAndStoreKPI_5min(self):
        print("Debut Export csv ...")
        left  = self.Histo_ohlc_5min
        right = self.HistoMinMaxDf.loc[(self.HistoMinMaxDf['TailleBougie']=='5min')].copy()
        right.drop(columns=['EMA20','EMA50','xclose'],inplace=True)
        key=['Contrat','Ts']
        SuiviTotalDf = pd.merge(left, right, on=key,how='left',suffixes=("_H", "_L"))

        left  = SuiviTotalDf
        right = self.TabSuiviKPI.loc[(self.TabSuiviKPI['Periode']=='5min')].copy()
        right.drop(columns=['ts','LAST_PRICE'],inplace=True)
        right.rename(columns={ 'LAST_BOUGIE': 'Ts'}, inplace=True)
        # print(left.info())
        # print(right.info())
        right['Ts'] = right['Ts'].apply(lambda x: x)
        right = right.drop_duplicates(subset=['Contrat','Ts'], keep='last')
        key=['Contrat','Ts']
        SuiviTotalDf = pd.merge(left, right, on=key,how='left',suffixes=("_H", "_L"))
        SuiviTotalDf.sort_values(['Contrat','Ts'], ascending=False, inplace=True)

        # SuiviTotalDf['Cible'] = getCible(SuiviTotalDf['xcouleur','Dist_EMA20','Dist_EMA50','R0','R1','R2','S0','S1','S2'])

        SuiviTotalDf[['EMA20','EMA50','EMA05','RSI14','xopen','xlow','xhigh','Pente_EMA50_5','Pente_EMA05_2','Dist_EMA20','Dist_EMA50']] = SuiviTotalDf[['EMA20','EMA50','EMA05','RSI14','xopen','xlow','xhigh','Pente_EMA50_5','Pente_EMA05_2','Dist_EMA20','Dist_EMA50']].astype(float)
        
        SuiviTotalDf.to_csv("C:\\Temp\\SuiviTotalDf.csv",sep=';',decimal='.',float_format='%.1f', index=False)
        # print("Export csv OK...")
        
    def getAndStoreKPI(self):
        # print("Debut Export csv ...")

        Histo_ohlc_day = self.Histo_ohlc_day.copy()
        Histo_ohlc_day.rename(columns={ 'Date': 'Ts'}, inplace=True)
        Histo_ohlc_day['TailleBougie'] = '1D'
        
        Histo_ohlc_15min = self.Histo_ohlc_15min.copy()
        Histo_ohlc_15min['TailleBougie'] = '15min'
        
        Histo_ohlc_5min = self.Histo_ohlc_5min.copy()
        Histo_ohlc_5min['TailleBougie'] = '5min'

        Histo_ohlc_1min = self.Histo_ohlc_1min.copy()
        Histo_ohlc_1min['TailleBougie'] = '1min'

        Histo_ohlc = pd.concat([Histo_ohlc_day, Histo_ohlc_15min, Histo_ohlc_5min, Histo_ohlc_1min])
        TailleBougie_order = CategoricalDtype(['1D', '15min', '5min', '1min'], ordered=True)
        Histo_ohlc['TailleBougie'] = Histo_ohlc['TailleBougie'].astype(TailleBougie_order) 

        left  = Histo_ohlc
        right = self.HistoMinMaxDf.copy()
        # print('***'*20)
        # print('1:')
        # print(left.info())
        # print(right.info())        
        right.drop(columns=['EMA20','EMA50','xclose'],inplace=True)
        key=['Contrat','Ts','TailleBougie']
        SuiviTotalDf = pd.merge(left, right, on=key,how='left',suffixes=("_H", "_L"))

        left  = SuiviTotalDf
        right = self.TabSuiviKPI.copy()
        right.drop(columns=['ts','LAST_PRICE'],inplace=True)
        right.rename(columns={ 'LAST_BOUGIE': 'Ts'}, inplace=True)
        right.rename(columns={ 'Periode': 'TailleBougie'}, inplace=True)
        right['TailleBougie'] = right['TailleBougie'].astype(TailleBougie_order) 
        
        # print('***'*20)
        # print('2:')
        # print(left.info())
        # print(right.info())
        right['Ts'] = right['Ts'].apply(lambda x: x)
        right = right.drop_duplicates(subset=['Contrat','Ts','TailleBougie'], keep='last')
        key=['Contrat','Ts','TailleBougie']
        SuiviTotalDf = pd.merge(left, right, on=key,how='left',suffixes=("_H", "_L"))
        SuiviTotalDf.sort_values(['Contrat','Ts','TailleBougie'], ascending=False, inplace=True)

        # SuiviTotalDf['Cible'] = getCible(SuiviTotalDf['xcouleur','Dist_EMA20','Dist_EMA50','R0','R1','R2','S0','S1','S2'])

        SuiviTotalDf[['EMA20','EMA50','EMA05','RSI14','xopen','xlow','xhigh','Pente_EMA50_5','Pente_EMA05_2','Dist_EMA20','Dist_EMA50']] = SuiviTotalDf[['EMA20','EMA50','EMA05','RSI14','xopen','xlow','xhigh','Pente_EMA50_5','Pente_EMA05_2','Dist_EMA20','Dist_EMA50']].astype(float)
        
        # print('*'*100)
        # print('SuiviTotalDf avant modif :')
        # print(SuiviTotalDf)
        
        SuiviTotalDf['R0'] = SuiviTotalDf['R0'].apply(lambda x: literal_eval(x) if type(x)==str else x)
        SuiviTotalDf['R1'] = SuiviTotalDf['R1'].apply(lambda x: literal_eval(x) if type(x)==str else x)
        SuiviTotalDf['R2'] = SuiviTotalDf['R2'].apply(lambda x: literal_eval(x) if type(x)==str else x)
        SuiviTotalDf['S0'] = SuiviTotalDf['S0'].apply(lambda x: literal_eval(x) if type(x)==str else x)
        SuiviTotalDf['S1'] = SuiviTotalDf['S1'].apply(lambda x: literal_eval(x) if type(x)==str else x)
        SuiviTotalDf['S2'] = SuiviTotalDf['S2'].apply(lambda x: literal_eval(x) if type(x)==str else x)
        
        # print('*'*100)
        # print('SuiviTotalDf apres modif:')
        # print(SuiviTotalDf)
        
        self.SuiviGlobalDf = SuiviTotalDf
        
        # SuiviTotalDf.to_csv("C:\\Temp\\SuiviTotalDf.csv",sep=';',decimal='.',float_format='%.1f', index=False)
        # print("Export csv OK...")
        
        # Define a function that handles and parses psycopg2 exceptions
        def show_psycopg2_exception(err):
            # get details about the exception
            err_type, err_obj, traceback = sys.exc_info()    
            # get the line number when exception occured
            line_n = traceback.tb_lineno    
            # print the connect() error
            print ("\npsycopg2 ERROR:", err, "on line number:", line_n)
            print ("psycopg2 traceback:", traceback, "-- type:", err_type) 
            # psycopg2 extensions.Diagnostics object attribute
            print ("\nextensions.Diagnostics:", err.diag)    
            # print the pgcode and pgerror exceptions
            print ("pgerror:", err.pgerror)
            print ("pgcode:", err.pgcode, "\n")
            
        # Define function using copy_from() with StringIO to insert the dataframe
        def copy_from_dataFile_StringIO(conn, datafrm, table):
            
          # save dataframe to an in memory buffer
            buffer = StringIO()
            datafrm.to_csv(buffer, header=False, index = False, sep=';',decimal='.')
            buffer.seek(0)
            
            cursor = conn.cursor()
            try:
                cursor.copy_from(buffer, table, sep=';', null="")
                # print("Data inserted using copy_from_datafile_StringIO() successfully....")
            except (Exception, psycopg2.DatabaseError) as err:
                # pass exception to function
                show_psycopg2_exception(err)
                cursor.close()
            conn.commit()
            cur.close()
        
        conn = psycopg2.connect("host= 127.0.0.1 dbname=Trading user=postgres password=admin")
        #"host= 127.0.0.1 dbname=testdb user=postgres password=postgres")
        # print("Connecting to Database")
        # FichierJ = csvPath + "SuiviTotalDf.csv"
        # df = pd.read_csv(FichierJ, sep=';',decimal='.')

        #Suppression des lignes :
        sqlQueryDelete = 'DELETE FROM suivitotaldf ;\n '
        cur = conn.cursor()
        cur.execute(sqlQueryDelete)
        conn.commit()
        # print("Vidage table suivitotaldf OK...")

        copy_from_dataFile_StringIO(conn, SuiviTotalDf, "suivitotaldf")

    
        

bot = Bot()
