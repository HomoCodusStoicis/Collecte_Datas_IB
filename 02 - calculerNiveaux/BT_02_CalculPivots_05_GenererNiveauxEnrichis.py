#*****************************************************************************
#  CalculPivots_GenererNiveauxEnrichis.py
#
#  Production fichier des points pivots + niveaux symboliques pour un contrat donné
#
#       Calcul à partir des fichiers PointsPivots
#       
#   Sortie : 1 fichier par Contrat/Date : exemple : HistoHLCS_Pivots_Niveaux_DAX-mini_2022-03-14.csv
#
#   les fichiers déjà existants sont sauvegardés et regénérés
#*****************************************************************************
import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

DateInDebStr = "2022-05-05"
DateInFinStr = HierStr
#DateInFinStr = "2022-06-13"


repertoireIn  = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\05 - Histo Pivots'
repertoireOut = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\06 - Niveaux enrichis'


import pandas as pd
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import collections
import inspect

import Niveaux

import numpy as np

import shutil

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 300)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:0.1f}'.format
    

seuilFusionNiveaux = 0 # ecart de points sous lesquel on fusionnera 2 niveaux trop proches

DateInDebDt=datetime.datetime.strptime(DateInDebStr, '%Y-%m-%d')
DateInFinDt=datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d')

DateFinStr    = DateInFinDt.strftime('%Y-%m-%d')

FichierPivots  = repertoireIn  + "\\HistoHLCS_Pivots.csv"

try:
    with open(FichierPivots): 
        print("Chargement points pivots depuis fichier " + FichierPivots)
        Pivots = pd.read_csv(FichierPivots, sep=';',decimal='.',parse_dates=['Date','JourPrec'])

except IOError:
        print("Fichier inexistant:", FichierPivots, "...")
        FichierPivotsOK = False
        quit()


DateCourDt = DateInDebDt
while DateCourDt <= DateInFinDt:

    DateCourStr    = DateCourDt.strftime('%Y-%m-%d')

    print(" "*2000)
    # print("============================================================="*2)
    print("============================================================="*2)
    print("           Traitement journée du ", DateCourStr)
    # print("============================================================="*2)
    print("============================================================="*2)

    for iContrat in Pivots['Contrat'].unique():

        print("Traitement contrat: " + iContrat)
        F_c = (Pivots['Contrat']==iContrat)
        iPivots = Pivots.loc[F_c]

        F=(iPivots['Date']==DateCourDt)
        taille=iPivots[F].shape[0]
        
        if taille==1:
    
            NowStr = datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")
            FichierNiveaux = repertoireOut + "\\HistoHLCS_Pivots_Niveaux_" + iContrat + "_" + DateCourStr + ".csv"
            FichierNiveauxBackup     = repertoireOut + "\\Backup\\HistoHLCS_Pivots_Niveaux_" + iContrat + "_" + DateCourStr + "-" + NowStr + ".bac"
            try:
                with open(FichierNiveaux): 
                        print("Fichier déjà existant : " + FichierNiveaux )
                        print('Backup du fichier '+ FichierNiveaux + ' en ' + FichierNiveauxBackup)
                        shutil.move(FichierNiveaux, FichierNiveauxBackup)
                        # quit()
                        FICOK = True
            except IOError:
                    FICOK = True

            if FICOK:
    
                NiveauxCalcules = pd.DataFrame(columns=['Nom','Prix'])
                
                df=iPivots.loc[F]
                # print("==="*20)
                # print(df)
                
                for i in range(4,8,1):
                    NiveauxCalcules=NiveauxCalcules.append(pd.DataFrame([[df.iloc[:,i].name,df.iloc[:,i].values[0]]],columns=['Nom','Prix']),ignore_index=True)
                for i in range(11,15,1):
                    NiveauxCalcules=NiveauxCalcules.append(pd.DataFrame([[df.iloc[:,i].name,df.iloc[:,i].values[0]]],columns=['Nom','Prix']),ignore_index=True)
                for i in range(18,22,1):
                    NiveauxCalcules=NiveauxCalcules.append(pd.DataFrame([[df.iloc[:,i].name,df.iloc[:,i].values[0]]],columns=['Nom','Prix']),ignore_index=True)
                for i in range(23,74,1):
                    NiveauxCalcules=NiveauxCalcules.append(pd.DataFrame([[df.iloc[:,i].name,df.iloc[:,i].values[0]]],columns=['Nom','Prix']),ignore_index=True)
            
                #print(NiveauxCalcules)
        
                NiveauDuJour=Niveaux.Niveaux()
                NiveauDuJour.Init_Niveaux(NiveauxCalcules, seuilFusionNiveaux, FichierNiveaux)
                NiveauxEnrichis = NiveauDuJour.get_NiveauxEnrichis()
                NiveauxEnrichis['Contrat'] = iContrat
                NiveauxEnrichis['Date']    = DateCourStr
                #print(NiveauxEnrichis)
                df2 = NiveauxEnrichis[['Contrat','Date','Nom','Prix']]
                df2.rename(columns={'Nom': 'Niveau'},inplace=True)
                df2.to_csv(FichierNiveaux,sep=';',decimal='.',float_format='%.1f', index=False)
            
        else:
            print('taille!=1: ', taille)
        #FichierNiveaux = pathNiveaux + DateStr + '-NiveauxEnrichis.csv'

 
    DateCourDt = DateCourDt + datetime.timedelta(days=1)
    

print("Fin...")



