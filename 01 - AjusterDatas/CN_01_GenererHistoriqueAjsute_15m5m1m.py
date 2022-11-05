# -*- coding: utf-8 -*-
"""
Created on Sat Dec 11 15:42:41 2021

@author: fcour
"""

#*****************************************************************************
#  CN_01_GenererHistoriqueAjsute_15m5m1m.py
#
#  Production fichier des HLCS 15m-5m-1m réajustés sur les 2 derniers mois pour une date donnée
#
#      pour chaque fichier Q des historiques réajustés en bar Q, qui contient :
#       - l'échéance à prendre en compte    
#       - la correction à appliquer pour avoir la valeur réajusté    
#
#   Sortie : 1 fichier par Periode/Date : exemple : HistoBars_1min_Réajustés_pour_le 2022-01-01.csv
#
#  Les fichiers existants sont sauvegardés puis écrasés
#
#*****************************************************************************
import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")


# Jours pour lesquels on veut calculer l'hitorique réajusté :
DateInDebStr="2022-09-24"
DateInFinStr=HierStr
# DateInFinStr="2022-01-26"


repertoireQuoIn  = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\02 - Historiques quotidiens réajustés'
repertoireMinIn  = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\01 - Histo Bars Minutes'
repertoireOut    = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\02 - Histo Bars Minutes réajustés'


import argparse
import datetime
import collections
import inspect
from dateutil.relativedelta import relativedelta

import logging
import time as tm
import os.path
from dateutil.relativedelta import *
import shutil

import pandas as pd
from math import pi

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 300)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:0.1f}'.format


DateInDebDt=datetime.datetime.strptime(DateInDebStr, '%Y-%m-%d')
DateInFinDt=datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d')
res = pd.DataFrame()

    #-----------------------------------------------------------------------------
    # 1 - Lecture du fichier HLCS réajusté pour chaque journée à traiter:
    #-----------------------------------------------------------------------------

iDateDt = DateInDebDt

while iDateDt <= DateInFinDt:

    print("-------------------"*5)
    print("Traitement " + iDateDt.strftime('%Y-%m-%d'))
    print("-------------------"*5)
    iDateStr = iDateDt.strftime('%Y-%m-%d')  
    res = None    
   
    #-----------------------------------------------------------------------------
    # 2 - Parcours de toutes les lignes et ouverture des fichiers 15m-5m-1 associés
    #-----------------------------------------------------------------------------
    print("")
    print("--> Chargement des fichiers data :")
    print("")
    
    FichierHistoHLCS = repertoireQuoIn + '\\HistoHLCS_Réajustés_pour_le_' + iDateStr + '.csv'
    print("Chargement fichier : " + FichierHistoHLCS )

    try:
        with open(FichierHistoHLCS): 
            FICOK = True
    except IOError:
            FICOK = False
            print("Fichier inexistant : " , FichierHistoHLCS)
        
        
    if FICOK:
        df = pd.read_csv(FichierHistoHLCS, sep=';',decimal='.',parse_dates=['Date'])
            
    for i in df.index:
        # print("iContrat")
        # print(iContrat)
        Future_NomContrat        = df.loc[i,['Contrat']].values[0]
        Future_EcheanceContrat   = df.loc[i,['Echeance']].values[0]
        Future_Date              = df.loc[i,['Date']].values[0]
        Future_Date_Str          = Future_Date.strftime('%Y-%m-%d')
        Future_AppliedAjustement = df.loc[i,['AppliedAjustement']].values[0]
        #print("Contrat suivant : " + Future_NomContrat + "- Ech"+ Future_EcheanceContrat + " - Date deb:" , Future_DateRolloverDeb , " - Date fin: " , Future_DateRolloverFin )
 
        iRepMinutes = '\\HistoBars_' + Future_NomContrat + "-Ech"+ str(Future_EcheanceContrat)

        for iPer in ['15min', '5min', '1min']:

            iFicMinutes = '\\HistoBars-' + iPer + '-' + Future_NomContrat + "-Ech"+ str(Future_EcheanceContrat) + '-Q' + Future_Date_Str + '.csv'
            
            FichierMinutesIn    =    repertoireMinIn + iRepMinutes + iFicMinutes
            print("Chargement fichier : " + FichierMinutesIn )
        
            try:
                with open(FichierMinutesIn): 
                    FICOK = True
            except IOError:
                    FICOK = False
                    print("Fichier inexistant : " , FichierMinutesIn)
                
                
    #-----------------------------------------------------------------------------
    # 3 - Application de l'ajustement adéquat:
    #-----------------------------------------------------------------------------
            if FICOK:
                iBarsIn = pd.read_csv(FichierMinutesIn, sep=';',decimal='.',parse_dates=['Ts'])
                print("Nb lignes lues:", len(iBarsIn))
                iBars = iBarsIn.copy()
                iBars['open'] = iBars['open']  + Future_AppliedAjustement
                iBars['high'] = iBars['high']  + Future_AppliedAjustement
                iBars['low']  = iBars['low']   + Future_AppliedAjustement
                iBars['close']= iBars['close'] + Future_AppliedAjustement
                iBars['periode']= iPer
                iBars['AppliedAjustement']= Future_AppliedAjustement
                iBars['Contrat']= iBars['Contrat'].map({'NASDAQ-mini':'NQ', 'DOW-mini':'YM', 'DAX-mini':'DXM'})
                
                res = pd.concat([res,iBars])
            
    
    #-----------------------------------------------------------------------------
    # 4 - Ecriture des fichiers 15m, 5m et 1m pour la journée traitée:
    #-----------------------------------------------------------------------------
    for iPer in ['15min', '5min', '1min']:

        MoisFinStr    = iDateDt.strftime('%Y-M%m')
        NowStr = datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")
        
        iRepOut = '\\' + MoisFinStr
        iFicOut = '\\HistoBars_' + iPer + '_Réajustés_pour_le ' + iDateStr + '.csv'
        iRepBac = '\\Backup\\' + MoisFinStr
        iFicBac = '\\HistoBars_' + iPer + '_Réajustés_pour_le ' + iDateStr + "-" + NowStr + ".bac"
        
        
        FichierMinutesOut    =    repertoireOut + iRepOut + iFicOut
        FichierMinutesBac    =    repertoireOut + iRepBac + iFicBac
        print("Ecriture fichier : " + FichierMinutesOut )

        if not os.path.exists(repertoireOut + iRepOut):
            os.makedirs(repertoireOut + iRepOut)
    
        try:
            with open(FichierMinutesOut): 
                    print("Fichier déjà existant : " + FichierMinutesOut )
                    print('Backup du fichier '+ FichierMinutesOut + ' en ' + FichierMinutesBac)
                    if not os.path.exists(repertoireOut + iRepBac):
                        os.makedirs(repertoireOut + iRepBac)
                    shutil.move(FichierMinutesOut, FichierMinutesBac)
                    # quit()
                    FICOK = True
        except IOError:
                FICOK = True


        F=(res['periode']==iPer)
        res.loc[F].to_csv(FichierMinutesOut,sep=';',decimal='.',float_format='%.1f', index=False)
        print("Fichier écrit: ", FichierMinutesOut)
        
    iDateDt = iDateDt + datetime.timedelta(days=1)

print("Fin...")
     