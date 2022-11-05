# -*- coding: utf-8 -*-
"""
Created on Sat Dec 11 15:42:41 2021

@author: fcour
"""

#*****************************************************************************
#  CN_00_GenererHistoriqueAjsute.py.py
#
#  Production fichier des HLCS Q réajustés sur les 2 derniers mois pour une date donnée
#
#       H High:   source histo bar quotidien
#       L Low :   source histo bar quotidien
#       C Close:  source histo bar 30min     (last  du Q)
#       S Settle: source histo bar quotidien (close du Q)
#
#  Sortie : 1 fichier par date (jour) comprenant tout l'historique réajusté pour cette date 
#
#  Les fichiers existants sont écrasés
#
#*****************************************************************************
import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")



# Jours pour lesquels on veut calculer l'historique réajusté :
DateInDebStr="2022-09-25"
DateInFinStr=HierStr
# DateInFinStr="2022-01-26"

# Date origine de l'historique à prendre en compte :
DateInDebHistoStr="2021-12-09"


repertoireIn  = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\02 - Histo HLCS Quotidiens'
repertoireOut = 'Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\02 - Historiques quotidiens réajustés'



#ListeContratsIn = [["Contrat",["date RollOver echeance 1","date RollOver echeance 2",...]],
# Constatation emprique sur ProRealTime : la date de rollover est :
# - pour le NQ et le YM : le vendredi 8j avant la date d'xpiration de l'échéance. 
#   Exemple pour l'échance 2021-12-17 du YM : le rollover (bascule de l'éch 202112 à 202203 se fait le 10-12-2021
# - pour le DXM : le jour de la date d'expiration

ListeContratsIn = [["NASDAQ-mini",["20211210","20220311","20220610","20220909","20221209"]],
                   ["DOW-mini"   ,["20211210","20220311","20220610","20220909","20221209"]],
                   ["DAX-mini"   ,["20211217","20220318","20220617","20220915","20221216"]]
                 ]




import argparse
import datetime
import collections
import inspect
from dateutil.relativedelta import relativedelta

import logging
import time as tm
import os.path
from dateutil.relativedelta import *

import pandas as pd
from math import pi

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 300)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:0.1f}'.format


DateInDebDt=datetime.datetime.strptime(DateInDebStr, '%Y-%m-%d')
DateInFinDt=datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d')

iDateDt = DateInDebDt

while iDateDt <= DateInFinDt:

    print("-------------------"*5)
    print("Traitement " + iDateDt.strftime('%Y-%m-%d'))
    print("-------------------"*5)
    DateIn    = iDateDt.strftime('%Y-%m-%d')  
    
    DateFinDt    = datetime.datetime.strptime(DateIn, '%Y-%m-%d')
    DateFinMaxDt = DateFinDt + datetime.timedelta(days=+120)
    # DateDebDt1M  = DateFinDt + relativedelta(months=-3)##############################################################
    # DateDebDt    = datetime.datetime(DateDebDt1M.year,DateDebDt1M.month,1)
    DateDebDt    = datetime.datetime.strptime(DateInDebHistoStr, '%Y-%m-%d')
    #print(DateDebDt)
    DateDebStr    = DateDebDt.strftime('%Y-%m-%d')
    DateFinStr    = DateFinDt.strftime('%Y-%m-%d')
    DateFinMaxStr = DateFinMaxDt.strftime('%Y-%m-%d')
    MoisDebStr    = DateDebDt.strftime('%Y%m')
    MoisFinStr    = DateFinDt.strftime('%Y%m')
    MoisFinMaxStr = DateFinMaxDt.strftime('%Y%m')
    
    #-----------------------------------------------------------------------------
    # 1 - Selection des bonnes échéances de contrats :
    #-----------------------------------------------------------------------------
    print("")
    print("--> Contrats à prendre en compte :")
    print("")
    Contrats = pd.DataFrame(columns=['Contrat','Echeance','DateRolloverDebut','DateRolloverFin','A_Reajuster'])
    ListeContrats = []
    NomContratPrec = ''
    DateRolloverPrec = None
    for i in ListeContratsIn:
        for j in i[1]: #Dates rollover
            #print(j)
            jM = j[:6]
            NomContrat      = i[0]
            EcheanceContrat = jM
            DateRolloverFin    = datetime.datetime.strptime(j, '%Y%m%d')+ datetime.timedelta(days=-1)
            if NomContrat == NomContratPrec:
                DateRolloverDebut = DateRolloverPrec + datetime.timedelta(days=1)
            else:
                DateRolloverDebut = DateRolloverFin + datetime.timedelta(days=-90)
            DateRolloverPrec = DateRolloverFin
            NomContratPrec   = NomContrat
            if (DateRolloverDebut <= DateFinDt and DateRolloverFin >= DateDebDt):
                Contrats = Contrats.append({'Contrat':NomContrat, 'Echeance':EcheanceContrat, 'DateRolloverDebut': DateRolloverDebut, 'DateRolloverFin': DateRolloverFin}, ignore_index=True)
                ListeContrats.append([i[0] , jM, j])
   
    Contrats = Contrats.sort_values(by=['Contrat', 'Echeance'], ascending=False)

    NomContratPrec = ''
    for i in Contrats.index:
        NomContrat = Contrats.loc[i,['Contrat']].values[0]
        if NomContrat == NomContratPrec:
             Contrats.loc[i,['A_Reajuster']] = "OUI"
        else:
            Contrats.loc[i,['A_Reajuster']] = "non"
        NomContratPrec = NomContrat
        
        
    #print(ListeContrats)
    Contrats.reset_index(inplace=True)
    Contrats.drop(columns=['index'],inplace=True)
    print(Contrats)
    
    #-----------------------------------------------------------------------------
    # 2 - Lecture des fichiers HLCS et stockage dans un unique df:
    #-----------------------------------------------------------------------------
    print("")
    print("--> Chargement des fichiers data :")
    print("")
    
    # df = pd.DataFrame(columns=['Contrat','Echeance','Date','highJ','lowJ','closeJ','settleJ','volJ','DateRolloverDeb','DateRolloverFin','AjustementApplicable'])
    df = pd.DataFrame()
    
    for i in Contrats.index:
        # print("iContrat")
        # print(iContrat)
        Future_NomContrat      = Contrats.loc[i,['Contrat']].values[0]
        Future_EcheanceContrat = Contrats.loc[i,['Echeance']].values[0]
        Future_DateRolloverDeb = Contrats.loc[i,['DateRolloverDebut']].values[0]
        Future_DateRolloverFin = Contrats.loc[i,['DateRolloverFin']].values[0]
        #print("Contrat suivant : " + Future_NomContrat + "- Ech"+ Future_EcheanceContrat + " - Date deb:" , Future_DateRolloverDeb , " - Date fin: " , Future_DateRolloverFin )
        
        FichierHistoHLCS    =    repertoireIn + '\\HistoHLCS_' + Future_NomContrat + "-Ech"+ Future_EcheanceContrat + '.csv'
        print("Chargement fichier : " + FichierHistoHLCS )
    
        try:
            with open(FichierHistoHLCS): 
                FICOK = True
        except IOError:
                FICOK = False
                print("Fichier inexistant : " , FichierHistoHLCS)
            
            
        if FICOK:
            df_i = pd.read_csv(FichierHistoHLCS, sep=';',decimal='.',parse_dates=['Date'])
            print("Nb lignes lues:", len(df_i))
            F_d1=(df_i["Date"] < DateFinDt + datetime.timedelta(days=+1))
            F_d2=(df_i["Date"] > DateDebDt + datetime.timedelta(days=-1))
            print("Nb lignes gardées:", len(df_i.loc[F_d1 & F_d2]))     
            df_i_2 = df_i.loc[F_d1 & F_d2].copy()
            df_i_2["DateRolloverDeb"] = Future_DateRolloverDeb
            df_i_2["DateRolloverFin"] = Future_DateRolloverFin
            df_i_2["AjustementApplicable"] = 0
            df = pd.concat([df,df_i_2])
            print("Nb lignes fichier concaténé:", len(df))
            
    # print("df brut :")
    # print(df)
    #-----------------------------------------------------------------------------
    # 3 - Calcul de l'Ajustement de l'historique à appliquer:
    #-----------------------------------------------------------------------------
    print("")
    print("--> Calcul des ajustements à appliquer - Contrats concernés :")
    print("")
    df = df.sort_values(by=['Contrat', 'Date', 'Echeance'], ascending=False)
    df.reset_index(inplace=True)
    df.drop(columns=['index'],inplace=True)
    # print("df avant calcul ajustement...")
    # print(df)  
    
    #Liste des contrats/echeances pour lesquels il faut faire le rollover :
    F=(Contrats['A_Reajuster'] == "OUI")
    #print("Liste des contrats/echeances pour lesquels il faut faire le rollover :")
    print(Contrats.loc[F])
    #print("df avant calcul ajustement...")
    df['DateDeb'] = df["Date"] == df["DateRolloverDeb"]
    df['DateFin'] = df["Date"] == df["DateRolloverFin"]
    #print(df)  
    
    for i in Contrats.loc[F].index:
        # print("Contrat N°:",i)
        # print(Contrats.loc[i])
        
        Future_NomContrat      = Contrats.loc[i,['Contrat']].values[0]
        Future_EcheanceContrat = Contrats.loc[i,['Echeance']].values[0]
        Future_DateRolloverDeb = Contrats.loc[i,['DateRolloverDebut']].values[0]
        Future_DateRolloverFin = Contrats.loc[i,['DateRolloverFin']].values[0]

        F_Contrat  = (df["Contrat"]  == Future_NomContrat)
        F_Echeance = (df["Echeance"] == int(Future_EcheanceContrat))
        F_DateDeb  = (df["DateDeb"] == True)
        F_DateFin  = (df["DateFin"] == True)

        for i_old in df.loc[F_Contrat & F_Echeance & F_DateFin].index:
            #print(i_old)
            i_new = i_old - 1
            #print(i_new)


        PrixNouvelleEcheance = df.loc[i_new,["closeJ"]].values[0]
        PrixAncienneEcheance = df.loc[i_old,["closeJ"]].values[0]
        ValeurReajustement = PrixNouvelleEcheance - PrixAncienneEcheance
        Future_NomContrat      = df.loc[i_old,["Contrat"]].values[0]
        Future_EcheanceContrat = df.loc[i_old,["Echeance"]].values[0]
        F_Contrat  = (df["Contrat"]  == Future_NomContrat)
        F_Echeance = (df["Echeance"] == Future_EcheanceContrat)
        AjustementDejaApplique = df.loc[F_Contrat & F_Echeance,["AjustementApplicable"]]
        df.loc[F_Contrat & F_Echeance,["AjustementApplicable"]] = AjustementDejaApplique + ValeurReajustement
        
    #print(df)
    
    
    #-----------------------------------------------------------------------------
    # 4 - Sélection des échéances à conserver suivant les dates :
    #-----------------------------------------------------------------------------
    Res = pd.DataFrame()
    Res = None
    
    for i in Contrats.index:
        i_Contrat = Contrats.loc[i,["Contrat"]].values[0]
        i_Echeance = int(Contrats.loc[i,["Echeance"]].values[0])
        i_DateDeb = Contrats.loc[i,["DateRolloverDebut"]].values[0]
        i_DateFin = Contrats.loc[i,["DateRolloverFin"]].values[0]
        #print("Traitement Contrat:",i_Contrat, i_Echeance,i_DateDeb, i_DateFin )
        F_Contrat  = (df["Contrat"]  == i_Contrat)
        F_Echeance = (df["Echeance"] == i_Echeance)
        F_Date1    = (df["Date"] >= i_DateDeb)
        F_Date2    = (df["Date"] <= i_DateFin)
        
        #print(df.loc[F_Contrat & F_Echeance & F_Date1 & F_Date2 ])
        
        Res = pd.concat([Res, df.loc[F_Contrat & F_Echeance & F_Date1 & F_Date2]])
        
    Res = Res.sort_values(by=['Contrat', 'Date', 'Echeance'], ascending=False)
    Res.drop(columns=['DateDeb','DateFin'],inplace=True)
    
    #-----------------------------------------------------------------------------
    # 5 - Application de l'ajustement :
    #-----------------------------------------------------------------------------
    print("")
    print("--> Dataframe final pour la journée du : " +  iDateDt.strftime('%Y-%m-%d'))
    print("")
    Res["openJ"] = Res["openJ"] + Res["AjustementApplicable"]
    Res["highJ"] = Res["highJ"] + Res["AjustementApplicable"]
    Res["lowJ"] = Res["lowJ"] + Res["AjustementApplicable"]
    Res["closeJ"] = Res["closeJ"] + Res["AjustementApplicable"]
    Res["settleJ"] = Res["settleJ"] + Res["AjustementApplicable"]
    Res.rename(columns={ 'AjustementApplicable': 'AppliedAjustement'}, inplace=True)
    
    print(Res)
    
    
    #-----------------------------------------------------------------------------
    # 6 - Stockage sous forme de fichier csv :
    #-----------------------------------------------------------------------------
    print("")
    print("--> Stockage dataframe sur disque :")
    print("")
    FichierS = repertoireOut + '\\HistoHLCS_Réajustés_pour_le_' + DateIn + '.csv'
    Res.to_csv(FichierS,sep=';',decimal='.',float_format='%.1f', index=False)
    print("Fichier écrit: ", FichierS)
    print("Fin...")
    
    iDateDt = iDateDt + datetime.timedelta(days=1)
     