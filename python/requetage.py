# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster

"""
1. lancer VM
2. se connecter en ssh
3. sur la VM: ccm start puis ccm status node1
   ccm node1 show
4.
!!!!!!!!!!! (cf sujet tp)
Pour acceder a ces ports depuis la machine hote on doit configurer une redirection des ports via
un tunnel SSH. Par exemple pour rediriger le port local 9042 vers le port 9042 du noeud 1 du
cluster (qui tourne sur l’ip 127.0.0.1) lancer la commande suivante:

[andrei@desktop ~]$ ssh -nNT -L 9042:127.0.0.1:9042 bigdata@192.168.56.101
"""

# CONNECTION: After the driver connects to one of these nodes it will automatically discover the rest of the nodes in the cluster and connect to them, so you don’t need to list every node in your cluster.
cluster = Cluster(['127.0.0.1']) # 127.0.0.1 parce qu'on a créé un tunnel ssh
session = cluster.connect()

from selection_villes import findListVilles

import datetime

def Requetage(SeismeLatitude,SeismeLongitude, timestampTdT):
    Villes=findListVilles(SeismeLatitude,SeismeLongitude)
    Intervalles=[timestampTdT]
    Result = []
    for i in range(10,70,10):
        timestampTdT = timestampTdT+datetime.timedelta(0,0,0,0,i,0,0)
        Intervalles.append(timestampTdT)
        #print timestampTdT

    for ville in Villes:
        for t in Intervalles:
            Result.append(session.execute("SELECT Tel, lat, long FROM Tsunami_test1 WHERE T = %s AND Id_Ville = %s;", (t, ville)))


    return Result


result = Requetage(35.0, 135.0, datetime.datetime(2015,01,01,23,44,56))
