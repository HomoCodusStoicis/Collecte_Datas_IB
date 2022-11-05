#*****************************************************************************
#  CalculPivots_CalculerPointsPivots.py
#
#  Production fichier des points pivots entre DateDeb et DateFin pour un contrat donné
#
#       Calcul à partir des fichiers Result
#       
# 1 seul fichier résultat, pour la plage complete des dates analysées, contenant 1 ligne par jour
# fichier résultat sauvegardé, puis écrasé
#
# les dates qui n'ont pas toutes les valeurs renseignées sont ignorées (données Q, S, M)
#*****************************************************************************
import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

DateInDebStr = "2021-12-15"
DateInFinStr = HierStr
# DateInFinStr = "2022-01-28"

repertoireIn  = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\04 - Histo HLCS JourSemaineMois'
repertoireOut = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\05 - Histo Pivots'

import pandas as pd
import argparse
import datetime
from dateutil.relativedelta import relativedelta
import collections
import inspect

import numpy as np

import shutil

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 300)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:0.1f}'.format


path='Y:\\TRAVAIL\\Mes documents\\Bourse\\Data\\Data API IB\\'
NowStr = datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")

FichierHLCS   = repertoireIn  + "\\HistoHLCS_Result.csv"
FichierPivots = repertoireOut + "\\HistoHLCS_Pivots.csv"
FichierPivotsBackup = repertoireOut + "\\Backup\\HistoHLCS_Pivots-" + NowStr + ".bac"


# Test fichier resultat déjà existant :
try:
    with open(FichierPivots): 
            print("Fichier déjà existant : " + FichierPivots )
            print('Backup du fichier '+ FichierPivots + ' en ' + FichierPivotsBackup)
            shutil.move(FichierPivots, FichierPivotsBackup)
except IOError:
        FICIN_OK = False


try:
    with open(FichierHLCS): 
        FICIN_OK = True
except IOError:
        FICIN_OK = False
        print("Fichier inexistant : " , FichierHLCS)
        exit


def get_PP(periode,df):

    # print(df.info())

    DateDt   = df.iloc[:,0].copy()
    JourPrec = df.iloc[:,1].copy()
    SemPrec  = df.iloc[:,2].copy()
    high     = df.iloc[:,4].copy()
    low      = df.iloc[:,5].copy()
    last     = df.iloc[:,6].copy()
    settle   = df.iloc[:,7].copy()
    contrat  = df.iloc[:,8].copy()
    
    print('Dans get_PP')
    
#    print(lig)
    # print(JourPrec)
    # print(settle)
    
    H=high
    L=low
    C=None    

    NumDaySem       = DateDt.apply(lambda x: x.weekday())
    NumMoisDateCour = DateDt.apply(lambda x: x.month)
    NumMoisDatePrec = JourPrec.apply(lambda x: x.month)
    
    
    '''
    Subtilité au niveau de la valeur à prendre pour le Close :
    
    - pour les points pivots quotidiens, prendre Close = Settle (valeur à la cloture du marché)
    
    - pour les points pivots hebdos, prendre :
        - le lundi : Close = Settle (valeur à la cloture du marché)
        - les autres jours : 
            - pour le DAX : Close = last (22h pour dax)
            - pour le NASDAQ et DOW JONES : close = settle
        => close = settle sauf de mardi à vendredi sur le Dax
    
    - pour les points pivots mensuels, prendre :
        - Pour le DAX :
            - le 1er jour ouvré du mois : Close = Settle (valeur à la cloture du marché)
            - les autres jours : close = last (22h pour dax)
        - Pour le Nasdaq et le Dow :
            -  Close = Settle (valeur à la cloture du marché)
        => close = settle sauf pour le Dax à partir du 02

    '''
    
    if periode=='J':
        C=settle
    
    if periode=='S':
        C=settle
        F_d=(NumDaySem != 0) # pas lundi
        F_c=(contrat=='DAX-mini')
        #print('C[F]')
        #print(C[F])
        C.loc[F_d & F_c]  = last.loc[F_d & F_c] 
        
        
    if periode=='M':
        C = settle
        F_d=(NumMoisDateCour == NumMoisDatePrec)
        F_c=(contrat=='DAX-mini')
        C.loc[F_d & F_c]  = last.loc[F_d & F_c] 

    #Backup
    # if periode=='S':
    #     C=last
    #     F=(NumDaySem==0)
    #     #print('C[F]')
    #     #print(C[F])
    #     C[F]  = settle[F] 
        
    # if periode=='M':
    #     C = settle
    #     F=(NumMoisDateCour == NumMoisDatePrec)
    #     C[F]  = last[F] 
        
        
    # backup 20/02/2022
    # if periode=='S':
    #     C=settle
    #     F=(NumDaySem==0)
    #     #print('C[F]')
    #     #print(C[F])
    #     C[F]  = last[F] 
    # if periode=='M':
    #     C = last
    #     F=(NumMoisDateCour == NumMoisDatePrec)
    #     C[F]  = settle[F] 


    
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
    
    return pd.concat([S4,mS4,S3,mS3,S2,mS2,S1,mS1,Pivot,mR1,R1,mR2,R2,mR3,R3,mR4,R4],axis=1)
  

if FICIN_OK :
    df = pd.read_csv(FichierHLCS, sep=';',decimal='.',parse_dates=['Date','JourPrec'])
    print("****"*20+' df:')    
    print(df.head())
    F=(np.isnan(df['settleM']))
    pivots=df[-F]
    # print(pivots)
    
    # Ajout Points Pivots Journaliers :
    colsIn =['Date','JourPrec','SemPrec','MoisPrec','highJ','lowJ','closeJ','settleJ','Contrat']
    dfIn=pivots[colsIn]

    dfOut = get_PP('J',dfIn)
    dfOut.reset_index(inplace=True)
    pivots.reset_index(inplace=True)
    pivots=pd.merge(pivots,dfOut, on="index")
    pivots.set_index('index',inplace=True)

    # print("****"*20+' pivots:')    
    # print(pivots.head())
    
    pivots.rename(columns={ 0: 'S4J',1:'mS4J',2:'S3J',3:'mS3J',4:'S2J',5:'mS2J',6:'S1J',7:'mS1J',8:'PivotJ',9:'mR1J',10:'R1J'}, inplace=True)
    pivots.rename(columns={ 11: 'mR2J',12:'R2J',13:'mR3J',14:'R3J',15:'mR4J',16:'R4J'}, inplace=True)

    # print(pivots)    

    # Ajout Points Pivots Semaine :
    colsIn =['Date','JourPrec','SemPrec','MoisPrec','highS','lowS','closeS','settleS','Contrat']
    dfIn=pivots[colsIn]

    dfOut = get_PP('S',dfIn)
    dfOut.reset_index(inplace=True)
    pivots.reset_index(inplace=True)
    pivots=pd.merge(pivots,dfOut, on="index")
    pivots.set_index('index',inplace=True)
    
    pivots.rename(columns={ 0: 'S4S',1:'mS4S',2:'S3S',3:'mS3S',4:'S2S',5:'mS2S',6:'S1S',7:'mS1S',8:'PivotS',9:'mR1S',10:'R1S'}, inplace=True)
    pivots.rename(columns={ 11: 'mR2S',12:'R2S',13:'mR3S',14:'R3S',15:'mR4S',16:'R4S'}, inplace=True)

    # print(pivots)    

    # Ajout Points Pivots Mensuels :
    colsIn =['Date','JourPrec','SemPrec','MoisPrec','highM','lowM','closeM','settleM','Contrat']
    dfIn=pivots[colsIn]

    dfOut = get_PP('M',dfIn)
    dfOut.reset_index(inplace=True)
    pivots.reset_index(inplace=True)
    pivots=pd.merge(pivots,dfOut, on="index")
    pivots.set_index('index',inplace=True)
    
    pivots.rename(columns={ 0: 'S4M',1:'mS4M',2:'S3M',3:'mS3M',4:'S2M',5:'mS2M',6:'S1M',7:'mS1M',8:'PivotM',9:'mR1M',10:'R1M'}, inplace=True)
    pivots.rename(columns={ 11: 'mR2M',12:'R2M',13:'mR3M',14:'R3M',15:'mR4M',16:'R4M'}, inplace=True)

    # print(pivots)    

    pivots.to_csv(FichierPivots,sep=';',decimal='.',float_format='%.1f', index=False)
    print("Fichier écrit OK : ", FichierPivots)
    print("Fin traitement...")
else:
    print("Fichier inexistant : STOP")



