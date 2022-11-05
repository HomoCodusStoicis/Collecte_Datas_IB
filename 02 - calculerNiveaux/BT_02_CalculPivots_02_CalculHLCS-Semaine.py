 #*****************************************************************************
#  BT_02_CalculPivots_02_CalculHLCS-Semaine.py
#
#  Production fichier des HLCS entre DateDeb et DateFin 
#
#       Calcul à partir des fichiers quotidiens réajustés
#
# si fichier déjà existant, on passe au jour suivant sans rien ecrire
#
#Sortie : 1 fichier par Jour
#
#*****************************************************************************
import datetime
HierStr = (datetime.datetime.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")

DateInDebStr="2022-09-04"
DateInFinStr=HierStr
# DateInFinStr = "2022-01-14"


repertoireIn  = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\02 - Historiques quotidiens réajustés'
repertoireOut = 'Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\\03 - Histo HLCS Hebdos'

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
    

#Future_NomContrat="DOW-mini"
# Future_NomContrat="DAX-mini"
# Future_EcheanceContrat="202112"

# path='Y:\TRAVAIL\Mes documents\Bourse\Data\Data API IB\02 - Historiques quotidiens réajustés' + Future_NomContrat
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
    FichierS    = repertoireOut + '\\HistoHLCS_Réajustés_pour_le_' + DateCourStr + '_S.csv'
    try:
        with open(FichierS): 
            print("Fichier déjà existant : " + FichierS )
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
        
            df['Semaine']=df['Date'].apply(lambda x: pd.Timestamp(x).strftime('A%Y-S%U'))

            dfH=df.groupby(['Contrat','Semaine']).agg({'openJ':'first','highJ':'max','lowJ': 'min','closeJ':'last','settleJ':'last','volJ':'sum'})
            d={'openJ':'openS','highJ':'highS', 'lowJ':'lowS', 'closeJ':'closeS', 'settleJ':'settleS','volJ':'volS'} 
            dfH.columns = dfH.columns.to_series().map(d) 
            dfH = dfH.reset_index() 
            print(dfH)
            dfH.to_csv(FichierS,sep=';',decimal='.',float_format='%.1f', index=False)
            print("Fichier écrit: ", FichierS)
            
        else:
            print("Fichier inexistant : " + FichierJ)
        
    iDateDt = iDateDt + datetime.timedelta(days=+1)



print("Fin...")
