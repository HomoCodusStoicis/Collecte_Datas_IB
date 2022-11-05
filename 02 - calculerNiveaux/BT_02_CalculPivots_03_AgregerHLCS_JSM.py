#*****************************************************************************
#  CalculPivots_AgregerHLCS-JSM.py
#
#  Production fichier des HLCS entre DateDeb et DateFin pour un contrat donné
#
#       Calcul à partir des fichiers quotidiens/semaine/mois
#        TODO : gestion des jours fériés
#
# 1 seul fichier résultat, pour la plage complete des dates analysées, contenant 1 ligne par jour
# fichier résultat sauvegardé, puis écrasé
#*****************************************************************************
import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

DateInDebStr = "2021-12-15"
DateInFinStr = HierStr
# DateInFinStr = "2022-01-28"

repertoireInJ = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\02 - Historiques quotidiens réajustés'
repertoireInS = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\03 - Histo HLCS Hebdos'
repertoireInM = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\03 - Histo HLCS Mensuels'
repertoireOut = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\04 - Histo HLCS JourSemaineMois'

import pandas as pd
import argparse
import datetime
import time as tm
from dateutil.relativedelta import relativedelta
import collections
import inspect

import pandas as pd

import shutil

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 300)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:0.1f}'.format



def CalculerJourSuivant(DateDt):
        #Jour suivant :
    #Si jour courant = vendredi, le jour suivant est lundi : 3j apres
    #Sinon c'est le suivant : 1j après
    NumDay = DateDt.weekday()
    if NumDay == 4:
        NbDays=3
    else:
        NbDays=1
    
    JourSuiv = (DateDt + datetime.timedelta(days=NbDays))         
        
    return JourSuiv


def CalculerSemPrec(DateDt):
    
    #Semaine précédente:
    SemPrec = (DateDt + datetime.timedelta(days=-7)).strftime('A%Y-S%U')

    return SemPrec
    
def CalculerMoisPrec(DateDt):
    
    #mois précédent :
    MoisPrec = (DateDt + relativedelta(months=-1)).strftime('A%Y-M%m')
    
    return MoisPrec



DateInDebDt=datetime.datetime.strptime(DateInDebStr, '%Y-%m-%d')
DateInFinDt=datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d')
DateDebStr    = DateInDebDt.strftime('%Y-%m-%d')
DateFinStr    = DateInFinDt.strftime('%Y-%m-%d')

tsStr = datetime.datetime.fromtimestamp(tm.time()).strftime("%Y%m%d %H-%M-%S")

FichierR       = repertoireOut + "\\HistoHLCS_Result.csv"
FichierRBackup = repertoireOut + "\\Backup\\HistoHLCS_Result-" + tsStr + ".bac"

# Test fichier resultat déjà existant :
try:
    with open(FichierR): 
        print("Fichier déjà existant : " +  FichierR )
        print('Backup du fichier '+ FichierR + ' en ' + FichierRBackup)
        shutil.move(FichierR, FichierRBackup)
        FichierATraiter = True
except IOError:
        FichierATraiter = True


if FichierATraiter:

    iDateDt = DateInDebDt
    
    while iDateDt <= DateInFinDt:
    
        NowStr = datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")
    
        DateCourDt  = iDateDt
        DateCourStr = DateCourDt.strftime('%Y-%m-%d')
        # SemCourStr  = DateCourDt.strftime('%Y-%m-%d')
        # MoisCourStr = DateCourDt.strftime('%Y-%m-%d')
    
        iFichierJ = repertoireInJ + '\\HistoHLCS_Réajustés_pour_le_' + DateCourStr + '.csv'
        iFichierS = repertoireInS + '\\HistoHLCS_Réajustés_pour_le_' + DateCourStr + '_S.csv'
        iFichierM = repertoireInM + '\\HistoHLCS_Réajustés_pour_le_' + DateCourStr + '_M.csv'
        print("Chargement fichier J : " + iFichierJ )
    
            
        try:
            with open(iFichierJ): 
                FICOK_J = True
        except IOError:
                FICOK_J = False
                print("Fichier inexistant : " , iFichierJ)
        
        try:
            with open(iFichierS): 
                FICOK_S = True
        except IOError:
                FICOK_S = False
                print("Fichier inexistant : " , iFichierS)
                
        try:
            with open(iFichierM): 
                FICOK_M = True
        except IOError:
                FICOK_M = False
                print("Fichier inexistant : " , iFichierM)
        
          
        
        if FICOK_J & FICOK_S & FICOK_M:
            dfJ = pd.read_csv(iFichierJ, sep=';',decimal='.',parse_dates=['Date'])
            dfS = pd.read_csv(iFichierS, sep=';',decimal='.')
            dfM = pd.read_csv(iFichierM, sep=';',decimal='.')
            
            dfJ.sort_values(by=['Contrat','Date'], ascending=False, inplace=True)
            dfS.sort_values(by=['Contrat','Semaine'], ascending=False, inplace=True)
            dfM.sort_values(by=['Contrat','Mois'], ascending=False, inplace=True)
                

            dfJ['JourSuiv'] = dfJ['Date'].shift(1)
            F = dfJ['Contrat'].shift(1) != dfJ['Contrat'] 
                
            # print(dfJ)
            # print('****'*50)
            #override first value de chaque contrat by CalculerJourSuivant
            #dfJ.loc[F,'JourSuiv']= CalculerJourSuivant(dfJ.loc[F,'Date'])
            #dfJ.loc[F,'JourSuiv']= dfJ.loc[F,'Date'].apply(lambda x: CalculerJourSuivant(x))
            
            for i in dfJ.loc[F].index:
                # print("index:",i)
                dfJ.loc[i,'JourSuiv']= CalculerJourSuivant(dfJ.loc[i,'Date'])
                
            #print(dfJ)
            dfJ['SemPrec']  = dfJ['JourSuiv'].apply(lambda x: CalculerSemPrec(x))
            dfJ['MoisPrec'] = dfJ['JourSuiv'].apply(lambda x: CalculerMoisPrec(x))
        
            dfJ.rename(columns={'Date': 'JourPrec'},inplace=True)
            dfJ2 = dfJ[['Contrat','JourSuiv','JourPrec','openJ','highJ','lowJ','closeJ','settleJ','volJ','SemPrec','MoisPrec']]
            dfJ2.rename(columns={'JourSuiv': 'Date'},inplace=True)
          
            dfS.rename(columns={'Semaine': 'SemPrec'},inplace=True)
            dfM.rename(columns={'Mois': 'MoisPrec'},inplace=True)
        
            # print(dfJ2)
            # print(dfS)
            # print(dfM)
            
            #Merge  :
            left= dfJ2
            right=dfS
            result=pd.merge(left,right,how='left', on=['Contrat','SemPrec'])
            
            left= result
            right=dfM
            result=pd.merge(left,right,how='left', on=['Contrat','MoisPrec'])
            
            
            colnames = result.columns.tolist()
            # print(colnames)
            colnames = colnames[:10] + colnames[11:17] + colnames[10:11] + colnames[17:]
            # print(colnames)
            result = result[colnames]
            
            # print(result)
            
            result.to_csv(FichierR,sep=';',decimal='.',float_format='%.1f', index=False)
        else:
            print("Fichier inexistant : traitement jour suivant...")
    
        iDateDt = iDateDt + datetime.timedelta(days=+1)


print("Fin...")

