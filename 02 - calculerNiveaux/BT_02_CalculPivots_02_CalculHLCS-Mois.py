 #*****************************************************************************
#  BT_02_CalculPivots_02_CalculHLCS-Mois.py
#
#  Production fichier des HLCS entre DateDeb et DateFin 
#
#       Calcul à partir des fichiers quotidiens réajustés
#
# si fichier déjà existant, on passe au jour suivant sans rien ecrire
#
#Sortie : 1 fichier par Jour
#*****************************************************************************
import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

DateInDebStr="2022-09-04"
DateInFinStr=HierStr
# DateInFinStr = "2022-01-14"

repertoireIn  = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\02 - Historiques quotidiens réajustés'
repertoireOut = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\03 - Histo HLCS Mensuels'

import pandas as pd
import argparse
import datetime
import collections
import inspect
import pandas as pd
import shutil

pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 300)
pd.set_option('display.width', 1000)
pd.options.display.float_format = '{:0.1f}'.format
    


NowStr = datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S")


DateInDebDt=datetime.datetime.strptime(DateInDebStr, '%Y-%m-%d')
DateInFinDt=datetime.datetime.strptime(DateInFinStr, '%Y-%m-%d')
DateDebStr    = DateInDebDt.strftime('%Y-%m-%d')
DateFinStr    = DateInFinDt.strftime('%Y-%m-%d')



iDateDt = DateInDebDt

while iDateDt <= DateInFinDt:

    DateCourDt  = iDateDt
    DateCourStr = DateCourDt.strftime('%Y-%m-%d')

    # Test fichier resultat déjà existant :
    FichierM    = repertoireOut + '\\HistoHLCS_Réajustés_pour_le_' + DateCourStr + '_M.csv'
    try:
        with open(FichierM): 
            print("Fichier déjà existant : " + FichierM )
            FichierATraiter = False
    except IOError:
            FichierATraiter = True
    
    if FichierATraiter:
        FichierJ = repertoireIn + '\\HistoHLCS_Réajustés_pour_le_' + DateCourStr + '.csv'
        
        
        try:
            with open(FichierJ): 
                #print("GetHisto = False")
                FICOK = True
        except IOError:
                #print("GetHisto = True")
                FICOK = False
        
        if FICOK:
            df = pd.read_csv(FichierJ, sep=';',decimal='.',parse_dates=['Date'])
            
            df.sort_values(by=['Contrat','Date'], ascending=True, inplace=True)

            df['Mois']=df['Date'].apply(lambda x: pd.Timestamp(x).strftime('A%Y-M%m'))
        
            dfH=df.groupby(['Contrat','Mois']).agg({'openJ':'first', 'highJ':'max','lowJ': 'min','closeJ':'last','settleJ':'last','volJ':'sum'})
            d={'openJ':'openM', 'highJ':'highM', 'lowJ':'lowM', 'closeJ':'closeM', 'settleJ':'settleM','volJ':'volM'} 
            dfH.columns = dfH.columns.to_series().map(d) 
            dfH = dfH.reset_index() 
            print(dfH)
            dfH.to_csv(FichierM,sep=';',decimal='.',float_format='%.1f', index=False)
            print("Fichier écrit: ", FichierM)
            
        else:
            print("Fichier inexistant : " + FichierJ)
        
    iDateDt = iDateDt + + datetime.timedelta(days=+1)



print("Fin...")
