# -*- coding: utf-8 -*-
"""
Created on Sat Feb 27 15:10:25 2021

@author: fcour
"""

import pandas as pd
import numpy as np
import time
import datetime

class Niveaux:
    
    def __init__(self):
        
        self.NivS1_H = None
        self.NivS1_L = None
        self.NIvAdj  = None
        self.NiveauxS1 = None
        

    
    def Init_Niveaux(self, Pivots, SeuilFusionNiveaux, NomFichierNiveauxEnrichi):
    
        # print('==================================================================================')
        # print('                      Package Niveaux.py - Init_Niveaux ')
        # print('==================================================================================')
        
        # Fichier csv avec tous les niveaux :
        #self.NiveauxS1 = pd.read_csv('Y:\TRAVAIL\Mes documents\Bourse\Python\Robot IB\Robot IB\\NiveauxQ.csv',
        #                        sep=';')
        
        
        #Fichier constitué à partir de l'historique des barres 5 minutes
        self.NiveauxS1 = Pivots
        
        
        # Ajout des niveaux 250 :
        Borne_Inf=  int(self.NiveauxS1['Prix'].min() / 250) - 1
        Borne_Sup=  int(self.NiveauxS1['Prix'].max() / 250) + 1
        #print(Borne_Inf)
        #print(Borne_Sup)
        j=self.NiveauxS1.index.max()+1
        #print(j)
        for i in range(Borne_Inf, Borne_Sup, 1):
            ligne=i*250
            self.NiveauxS1.loc[j] = [str(ligne), ligne]
            j=j+1
        
            
            
        print('--------------------------------------------')
        print('                  Niveaux.csv           ')
        print('--------------------------------------------')
        print(self.NiveauxS1)
        print('--------------------------------------------')

        self.NiveauxS1.sort_values(by=['Prix'],inplace=True)
        #self.NiveauxS1['Contrat'] = 

        #self.NiveauxS1.to_csv(NomFichierNiveauxEnrichi,sep=';',decimal='.',float_format='%.1f', index=False)
        
        ############################################################################################################################
        # Création de 2 df nettoyés :
        #   NivS1_H : liste des niveaux à prendre en compte en phase montante, en supprimant les niveaux intermédiaires trop proches
        #   NivS1_L : liste des niveaux à prendre en compte en phase descendante, en supprimant les niveaux intermédiaires trop proches
        ############################################################################################################################

        
        # self.NivS1_H=self.NiveauxS1.copy()
        # self.NivS1_L=self.NiveauxS1.copy()
        # self.NivS1_H.sort_values(by=['Prix'],inplace=True)
        # self.NivS1_L.sort_values(by=['Prix'],ascending=False, inplace=True)
        
        
        # self.NivS1_H.reset_index(inplace=True)
        # self.NivS1_L.reset_index(inplace=True)
        # self.NivS1_H.drop(columns=['index'],inplace=True)
        # self.NivS1_L.drop(columns=['index'],inplace=True)
        
        
        # for i in range (1, self.NivS1_H.shape[0]):
        #     if ( abs(self.NivS1_H.loc[i]['Prix'] - self.NivS1_H.loc[i-1]['Prix']) <= SeuilFusionNiveaux):
        #         self.NivS1_H.drop(i-1,0,inplace=True)
            
        # for i in range (1, self.NivS1_L.shape[0]):
        #     if ( abs(self.NivS1_L.loc[i]['Prix'] - self.NivS1_L.loc[i-1]['Prix']) <= SeuilFusionNiveaux):
        #         self.NivS1_L.drop(i-1,0,inplace=True)
        
            
        # self.NivS1_H.reset_index(inplace=True)
        # self.NivS1_L.reset_index(inplace=True)
        
        # self.NivS1_H.drop(columns=['index'],inplace=True)
        # self.NivS1_L.drop(columns=['index'],inplace=True)
        
        # print('--------------------------------------------')
        # print('                  NivS1_H           ')
        # print('--------------------------------------------')
        # print(self.NivS1_H)
        # print('--------------------------------------------')
        # print('                  NivS1_L           ')
        # print('--------------------------------------------')
        # print(self.NivS1_L)
        # print('--------------------------------------------')

    def get_df_histo_niveaux(self):
        
        '''
        print('==================================================================================')
        print('                      Package Niveaux.py - get_df_histo_niveaux ')
        print('==================================================================================')
        '''
        Niveaux = self.NiveauxS1.copy()
        Niveaux['TSAntePrec'] = None
        Niveaux['numTickAntePrec']=-10000
        Niveaux['TSPrec'] = None
        Niveaux['numTickPrec']=-10000
        Niveaux['OrderID']=0
        Niveaux.set_index('Nom',inplace=True)
        '''
        print('--------------------------------------------')
        print('                  Niveaux           ')
        print('--------------------------------------------')
        print(Niveaux)
        print('--------------------------------------------')
        '''
        return Niveaux
              
    def get_Niveaux_adjacents(self, prix_courant:float):
        
        '''
        print('==================================================================================')
        print('                      Package Niveaux.py - get_Niveaux_adjacents ')
        print('==================================================================================')
        '''
        #Recherche du niveau supérieur:
        FL=(self.NiveauxS1['Prix'] >= prix_courant)
        NivSup=self.NiveauxS1[FL].iloc[0]
             
        #Recherche du niveau inférieur:
        FL=(self.NiveauxS1['Prix'] <= prix_courant)
        NivInf=self.NiveauxS1[FL].iloc[-1]
        
        self.NIvAdj=pd.DataFrame([[NivSup.values[0], NivSup.values[1]],
                                  [NivInf.values[0], NivInf.values[1]]],
                             index = ['Sup+', 'Inf-'], columns = ['Nivo', 'Prix'])
    
        '''
        print('--------------------------------------------')
        print('              Niveaux Adjacents           ')
        print('--------------------------------------------')
        print(self.NIvAdj)
        print('--------------------------------------------')
        '''
        return self.NIvAdj
    
    def get_Niveaux_adjacents_a_trader(self, prix_courant:float, DistanceNiveau:float):
        
        '''
        print('==================================================================================')
        print('                      Package Niveaux.py - get_Niveaux_adjacents_a_trader ')
        print('==================================================================================')
        '''
        #Recherche du niveau supérieur:
        FL=(self.NivS1_H['Prix'] >= prix_courant)
        NivSup=self.NivS1_H[FL].iloc[0]
             
        #Recherche du niveau inférieur:
        FL=(self.NivS1_L['Prix'] <= prix_courant)
        NivInf=self.NivS1_L[FL].iloc[0]
        
        self.NIvAdj=pd.DataFrame([[NivSup.values[0], NivSup.values[1], NivSup.values[1] - DistanceNiveau],
                                  [NivInf.values[0], NivInf.values[1], NivInf.values[1] + DistanceNiveau]],
                             index = ['Sup+', 'Inf-'], columns = ['Nivo', 'Prix', 'Trigger'])
    
        '''
        print('--------------------------------------------')
        print('              Niveaux Adjacents à trader          ')
        print('--------------------------------------------')
        print(self.NIvAdj)
        print('--------------------------------------------')
        '''
        return self.NIvAdj

    
    def get_Niveaux_H(self):
        
        Niveaux = self.NivS1_H.copy()
        Niveaux['TickDernierCross'] = -10000
        Niveaux.set_index('Nom',inplace=True)      
        
        return Niveaux

    def get_Niveaux_L(self):
        
        Niveaux = self.NivS1_L.copy()
        Niveaux['TickDernierCross'] = -10000
        Niveaux.set_index('Nom',inplace=True)
         
        return Niveaux

    def get_NiveauxEnrichis(self):
        
        Niveaux = self.NiveauxS1.copy()
         
        return Niveaux
'''  
ts_now  = datetime.datetime.today()    
ts_prec_str = ts_now.strftime("%Y%m%d ")  + '10:30:00'
ts_prec  = datetime.datetime.strptime(ts_prec_str, '%Y%m%d  %H:%M:%S')
delta_TS = ts_now - ts_prec
delta_min = int(round(delta_TS.total_seconds() / 60))
print(delta_TS)
print(delta_min)

 
auj = datetime.datetime.today() - datetime.timedelta(days=0)
J_Cour = auj.strftime('%Y-%m-%d')
S_Cour = auj.strftime('A%Y-S%U')
M_Cour = auj.strftime('A%Y-M%m')
print(J_Cour)
print(S_Cour)
print(M_Cour)


ts = datetime.datetime.today()
NumDay = datetime.datetime.today().weekday()
if NumDay > 4 :
    NbDaysAgo=NumDay-4
else:
    NbDaysAgo=NumDay+3

FinDeSemaineDerniere    = (ts - datetime.timedelta(days=NbDaysAgo)).strftime("%Y%m%d") + " 23:00:00"  

print(FinDeSemaineDerniere)

 
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

Histo_Mes_Ordres = pd.DataFrame(columns=['ID','Niveau','Bracket','Sens','Prix','Nb','Etat','NbTrt','NbRest','PrixExec','Bilan'])
Histo_Mes_Ordres.set_index("ID", inplace=True)
nomNiveau="S2M"
prix=14552
nbLots=3
stop_profit_S1=3
stop_loss_S1=20
ID1=120
Histo_Mes_Ordres.loc[ID1]  =[nomNiveau, "Parent",     "BUY",  prix,                nbLots, "Demandé",0,nbLots,14552.3,3]
Histo_Mes_Ordres.loc[ID1+1]=["PlusBasJ", "TakeProfit", "SELL", prix+stop_profit_S1, nbLots, "Demandé",0,nbLots,0,0]
Histo_Mes_Ordres.loc[ID1+2]=[nomNiveau, "StopLOss",   "SELL", prix-stop_loss_S1,   nbLots, "Demandé",0,nbLots,0,0]
Histo_Mes_Ordres.loc[ID1+3]  =[nomNiveau, "Parent",     "BUY",  prix,                nbLots, "Demandé",0,nbLots,14552.3,3]
Histo_Mes_Ordres.loc[ID1+4]=["mR2M", "TakeProfit", "SELL", prix+stop_profit_S1, nbLots, "Demandé",0,nbLots,0,0]
Histo_Mes_Ordres.loc[ID1+5]=[nomNiveau, "StopLOss",   "SELL", prix-stop_loss_S1,   nbLots, "Demandé",0,nbLots,0,0]


#Histo_Mes_Positions = pd.DataFrame(columns=['ID','Niveau','Bracket','Sens','Prix','Nb','Etat','NbTrt','NbRest'])


print(Histo_Mes_Ordres)
Status="Submitted"
filled=1
remaining=0
Histo_Mes_Ordres.loc[ID1,['Etat','NbTrt','NbRest']] = [Status, filled, remaining]
print(Histo_Mes_Ordres)

print("==========================================================================")        
print("- orderStatus - Historique des Ordres positionnés ")        
print("==========================================================================")        

taille = Histo_Mes_Ordres.shape[0]
Titre = "ID Niveau  Bracket  Sens  Prix  Nb      Etat   NbTrt NbRest PrixExec Bilan"   
print(Titre)
for ID in Histo_Mes_Ordres.index:
    Niv = Histo_Mes_Ordres.loc[ID,'Niveau'] 
    Bra = Histo_Mes_Ordres.loc[ID,'Bracket'] 
    Sns = Histo_Mes_Ordres.loc[ID,'Sens']
    Prx = Histo_Mes_Ordres.loc[ID,'Prix'] 
    Nb1 = Histo_Mes_Ordres.loc[ID,'Nb']
    Ett = Histo_Mes_Ordres.loc[ID,'Etat'] 
    Nb2 = Histo_Mes_Ordres.loc[ID,'NbTrt'] 
    Nb3 = Histo_Mes_Ordres.loc[ID,'NbRest'] 
    Pr2 = Histo_Mes_Ordres.loc[ID,'PrixExec'] 
    Bil = Histo_Mes_Ordres.loc[ID,'Bilan']
    Ligne = "{:0F} {:s} {:s}  {:s}  {:0.1F}  {:0F} {:s}  {:0F} {:0F} {:0.1F}  {:0F}".format(ID, 
           Niv, Bra, Sns, Prx, Nb1, Ett, Nb2, Nb3, Pr2, Bil)
    print(Ligne)
 
print("==========================================================================")        



   
    

    
    


print('   Prix actuel {:s} du niveau supérieur : ecart de {:0.2F} points'.format("xxxxxx", 14223.6666667))

        

NiveauDuJour=Niveaux()
NiveauDuJour.Init_Niveaux(10)
print(NiveauDuJour.get_Niveaux_adjacents(13899.4).loc['Inf-','Prix'])

Histo_Croisements_Niveaux = pd.DataFrame(columns=['prix', 'ts', 'numTick'])
nom='R1M'
prix=14000
ts = datetime.datetime.today()
n=15
Histo_Croisements_Niveaux.loc[nom]=[prix, ts, n]
print(Histo_Croisements_Niveaux)
print(Histo_Croisements_Niveaux.loc[nom])
#print('************************')
#print(Histo_Croisements_Niveaux[(Histo_Croisements_Niveaux.index=='R1M')]['numTick'])
#print('************************')
#print(Histo_Croisements_Niveaux[(Histo_Croisements_Niveaux.index=='R1QM')]['numTick'])
#print('************************') 
print(NiveauDuJour.get_df_histo_niveaux())


ordres_lies = pd.DataFrame(columns=['ID_Lie_1','ID_Lie_2'])
ordres_lies.loc[151]=[152,149]
ordres_lies.loc[152]=151
print(ordres_lies.loc[151,"ID_Lie_2"])
print(ordres_lies)
'''