# -*- coding: utf-8 -*-
"""
07/03/2021

@author: fcour
"""

import pandas as pd
import numpy as np
import time
import datetime
#from datetime import datetime
pd.set_option('display.max_rows', 500)

DateStr = datetime.datetime.today().strftime("%Y-%m-%d")
FichierHorairesJour = 'Y:\TRAVAIL\Mes documents\Bourse\Python\Robot IB\Robot IB\Suivi exécutions\\Q' + DateStr + '-Horaires.csv'


class Horaires:
    
    def __init__(self):
        
        self.Horaires = None
        

    
    def Init_Horaires(self):
    
        print('==================================================================================')
        print('                      Package Horaires.py - Init_Horaires ')
        print('==================================================================================')
        
        # Fichier csv avec les Horaires standard en fonction des jours de la semaine :
        self.HorairesSemaine = pd.read_csv('Y:\TRAVAIL\Mes documents\Bourse\Python\Robot IB\Robot IB\\HorairesTradingSemaine.csv',
                                 sep=';')
        
        auj=datetime.datetime.today()
        auj_ts=datetime.datetime(auj.year,auj.month,auj.day)
        NumDay=auj.weekday()
        HDeb=self.HorairesSemaine.loc[NumDay,'Deb']
        HFin=self.HorairesSemaine.loc[NumDay,'Fin']
        HHDeb=HDeb[0:2]
        HHFin=HFin[0:2]
        MMDeb=HDeb[3:5]
        MMFin=HFin[3:5]
        TSDeb=auj_ts + datetime.timedelta(hours=int(HHDeb),minutes=int(MMDeb))
        TSFin=auj_ts + datetime.timedelta(hours=int(HHFin),minutes=int(MMFin))
       
        print('--------------------------------------------')
        print('              HorairesTradingSemaine.csv           ')
        print('--------------------------------------------')
        print(self.HorairesSemaine)
        print('--------------------------------------------')
        print("Heure début trading", TSDeb)
        print("Heure fin   trading", TSFin)

        # Création DF du jour : 1 ligne par période de 5 minutes, qui indique OK / KO
        self.Horaires = pd.DataFrame(columns=['ts5m', 'NiveauRisque', 'Commentaire'])
        
        delta5min = datetime.timedelta(minutes=5)
       
        ts5min = auj_ts
        for i in range(1, 289):
            if ts5min < TSDeb or ts5min >= TSFin:
                self.Horaires.loc[i] = [ts5min, "KO", "HorairesDuJour"]
            else:
                self.Horaires.loc[i] = [ts5min, "OK", ""]
            ts5min = ts5min + delta5min
           
 
        # Fichier csv avec les Horaires risqués de la journée courante :
        self.HorairesKO1 = pd.read_csv('Y:\TRAVAIL\Mes documents\Bourse\Python\Robot IB\Robot IB\\HorairesTradingKO1.csv',
                                 sep=';')
        # Fichier csv avec les Horaires risqués de la journée courante :
        self.HorairesKO2 = pd.read_csv('Y:\TRAVAIL\Mes documents\Bourse\Python\Robot IB\Robot IB\\HorairesTradingKO2.csv',
                                 sep=';')

        self.HorairesKO = self.HorairesKO1.append(self.HorairesKO2, ignore_index=True)
        #print(self.HorairesKO)

        print('--------------------------------------------')
        print('              HorairesTradingKO.csv           ')
        print('--------------------------------------------')
        print(self.HorairesKO)
        print('--------------------------------------------')


        for i in range(0,self.HorairesKO.shape[0]):
            HDeb=self.HorairesKO.loc[i,'Deb']
            HFin=self.HorairesKO.loc[i,'Fin']
            HHDeb=HDeb[0:2]
            HHFin=HFin[0:2]
            MMDeb=HDeb[3:5]
            MMFin=HFin[3:5]
            TSDeb=auj_ts + datetime.timedelta(hours=int(HHDeb),minutes=int(MMDeb))
            TSFin=auj_ts + datetime.timedelta(hours=int(HHFin),minutes=int(MMFin))
            
            F1=(self.Horaires['ts5m']>=TSDeb)
            F2=(self.Horaires['ts5m']<TSFin)
            self.Horaires.loc[F1 & F2,['NiveauRisque', 'Commentaire']]=["KO", self.HorairesKO.loc[i,'description']]
            
        F1 = (self.Horaires['NiveauRisque']=="OK")
        print('--------------------------------------------')
        print('               HorairesTrading.csv           ')
        print('--------------------------------------------')
        print(self.Horaires.loc[F1])
        print('--------------------------------------------')
        self.Horaires.set_index('ts5m', inplace=True)

        self.Horaires.to_csv(FichierHorairesJour,sep=';',decimal='.',float_format='%.1f')

    def get_Risque(self, time: int):
        
        
        #print('==================================================================================')
        #print('                      Package Horaires.py - get_Risque ')
        #print('==================================================================================')
        ts = datetime.datetime.fromtimestamp(time)
        m5=(ts.minute // 5)*5
        ts5min=datetime.datetime(ts.year,ts.month,ts.day,ts.hour,m5)
        NiveauRisque = self.Horaires.loc[ts5min]['NiveauRisque']
        Explication = self.Horaires.loc[ts5min]['Commentaire']
        #print("Est-il risqué de trader à ", ts, "?")
        #print("Pour la période", ts5min, ", le niveau de risque est : ", 
        #      NiveauRisque, " (cause : ", Explication, ")")
        #F = (self.Horaires['ts5m'] == ts5min)
        #print(self.Horaires)
        if NiveauRisque =="OK":
            return "OK"
        else:
            return "KO"

    def get_next_heure_OK_pour_trading(self):
        ts = datetime.datetime.fromtimestamp(time.time())
        m5=(ts.minute // 5)*5
        ts5min=datetime.datetime(ts.year,ts.month,ts.day,ts.hour,m5)
        F1=(self.Horaires.index >=ts5min)
        F2=(self.Horaires['NiveauRisque']=="OK")
        NBOK=self.Horaires.loc[F1 & F2].shape[0]
        #print(NBOK)
        if NBOK > 0 :
            NextHeureOK = self.Horaires.loc[F1 & F2].index[0].strftime("%H:%M")
            #print(NextHeureOK)
        else:
            NextHeureOK = "demain"
            #print(NextHeureOK)
        return NextHeureOK

        
'''
HorairesDuJour=Horaires()
HorairesDuJour.Init_Horaires()

HorairesDuJour.get_next_heure_OK_pour_trading()


print(HorairesDuJour.get_Risque(int(time.mktime(datetime.datetime.today().timetuple()))))

print(int(time.mktime(datetime.datetime.today().timetuple())))
time.sleep(10) 
print(time.mktime(datetime.datetime.today().timetuple()))

print(NiveauDuJour.get_Horaires_adjacents(13899.4).loc['Inf-','Prix'])

Histo_Croisements_Horaires = pd.DataFrame(columns=['prix', 'ts', 'numTick'])
nom='R1M'
prix=14000
ts = datetime.datetime.today()
n=15
Histo_Croisements_Horaires.loc[nom]=[prix, ts, n]
print(Histo_Croisements_Horaires)
print(Histo_Croisements_Horaires.loc[nom])
#print('************************')
#print(Histo_Croisements_Horaires[(Histo_Croisements_Horaires.index=='R1M')]['numTick'])
#print('************************')
#print(Histo_Croisements_Horaires[(Histo_Croisements_Horaires.index=='R1QM')]['numTick'])
#print('************************') 
print(NiveauDuJour.get_df_histo_Horaires())'''

'''
ordres_lies = pd.DataFrame(columns=['ID_Lie_1','ID_Lie_2'])
ordres_lies.loc[151]=[152,149]
ordres_lies.loc[152]=151
print(ordres_lies.loc[151,"ID_Lie_2"])
print(ordres_lies)
'''