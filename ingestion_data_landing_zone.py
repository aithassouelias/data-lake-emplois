"""
Ce script permet d'ingérer des fichiers depuis un répertoire source vers un répertoire cible (landing zone)
en filtrant les fichiers selon un pattern donné. Il enregistre également des métadonnées techniques dans un fichier CSV.

Cela représente la première étape du processus d'ingestion des données dans un data lake.
"""
from datetime import datetime
import os, fnmatch
import shutil
import csv

def Get_datetime():
    Result = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return(Result)

def copy_files_from_source_to_cible(myPathSource, myPattern, myPathCible):
    """
    Fonction d'ingestion des fichiers d'un repertoire source vers un repertoire cible
    en filtrant les fichiers selon un pattern (filtre) donné
    Args:
        myPathSource (str): Repertoire source
        myPattern (str): Pattern de filtrage des fichiers
        myPathCible (str): Repertoire cible
    """

    # Creation ou ouverture du fichier de metadonnees techniques
    path_file_metadata = "C:/TD_DATALAKE/DATALAKE/00_METADATA/metadata_technique.csv"
    
    # Vérifie si le fichier existe déjà
    file_exists = os.path.isfile(path_file_metadata)
    
    # Ouverture et écriture dans le fichier de métadonnées
    file_metadata = open(path_file_metadata, 'a', encoding="utf-8", errors="ignore", newline='')
    writer_metadata = csv.writer(file_metadata, delimiter=';', quotechar='"', quoting=csv.QUOTE_ALL, lineterminator='\n')
    
    # Écrit l'en-tête seulement si le fichier n'existe pas
    if not file_exists:
        writer_metadata.writerow(["object_id","colonne","valeur"])

    # Récupère le dernier object_id utilisé si le fichier existe
    if file_exists:
        with open(path_file_metadata, 'r', encoding="utf-8", errors="ignore") as f:
            last_id = max((int(row[0]) for row in csv.reader(f, delimiter=';') if row[0].isdigit()), default=0)
        object_id = last_id + 1
    else:
        object_id = 1

    myListOfFileSourceTmp = os.listdir(myPathSource)
    myListOfFileSource = []
    for myFileNameTmp in myListOfFileSourceTmp:  
        if fnmatch.fnmatch(myFileNameTmp, myPattern)==True:
            myListOfFileSource.append(myFileNameTmp)

    for myFileNameToCopy in myListOfFileSource: 
        myPathFileNameSource = myPathSource + "/" + myFileNameToCopy
        myPathFileNameCible = myPathCible + "/" + myFileNameToCopy
        shutil.copy(myPathFileNameSource, myPathFileNameCible)
        writer_metadata.writerow([object_id,"fichier_source",myPathFileNameSource])
        writer_metadata.writerow([object_id,"fichier_cible",myPathFileNameCible])
        writer_metadata.writerow([object_id,"date_ingestion",Get_datetime()])
        object_id += 1
    file_metadata.close()
    print("Ingestion des fichiers de type ", myPattern, " effectuée dans la landing zone ", myPathCible, "\n")

# Ingestion des fichiers dans la landing zone
copy_files_from_source_to_cible("C:/TD_DATALAKE/DATALAKE/0_SOURCE_WEB", "*INFO-EMP*.html", "C:/TD_DATALAKE/DATALAKE/1_LANDING_ZONE/LINKEDIN/EMP")
copy_files_from_source_to_cible("C:/TD_DATALAKE/DATALAKE/0_SOURCE_WEB", "*INFO-SOC*.html", "C:/TD_DATALAKE/DATALAKE/1_LANDING_ZONE/GLASSDOOR/SOC")
copy_files_from_source_to_cible("C:/TD_DATALAKE/DATALAKE/0_SOURCE_WEB", "*AVIS-SOC*.html", "C:/TD_DATALAKE/DATALAKE/1_LANDING_ZONE/GLASSDOOR/AVI")