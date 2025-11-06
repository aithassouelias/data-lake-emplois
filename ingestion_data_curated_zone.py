# -*- coding: utf-8 -*-
#======================================================================================
# Auteur: Eric KLOECKLE
# Etablissement: Université Lumière Lyon 2 - Bron
# Cursus: Master 2 BI&A : « Business Intelligence & Analytics »
# Domaine: « Gestion de données massives »
# Cours: Enseignement TD Datalake-House
#
#--------------------------------------------------------------------------------------
# 02-PHASE-2_Extraction_des_donnéees_descriptives_de_la_LANDINGZONE_vers_la_CURATED-ZONE_v0.01.py
#======================================================================================

from bs4 import BeautifulSoup
import csv
import pandas as pd
import fnmatch
import os
import re

#==============================================================================
#-- GLASSDOOR (AVIS) : Fonction renvoyant <Nom_entreprise>
#==============================================================================
def extraire_nom_entreprise_AVI(objet_html: BeautifulSoup):
    """
    Extrait le nom de l'entreprise depuis la page d'avis Glassdoor
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Nom de l'entreprise ou 'NULL' si non trouvé
    """
    try:
        # Si aucune méthode n'a fonctionné, chercher dans toute la page
        h1 = objet_html.find('span', attrs={'id': 'DivisionsDropdownComponent'})
        company_name = h1.get_text(strip=True) if h1 else 'NULL'

        return company_name
    except Exception as e:
        print(f"Erreur lors de l'extraction du nom de l'entreprise: {str(e)}")
        return 'NULL'

#==============================================================================
#-- GLASSDOOR (AVIS) : Fonction renvoyant <Note_moy_entreprise>
#==============================================================================
def extraire_note_moy_entreprise_AVI(objet_html):
    """
    Extrait la note moyenne de l'entreprise depuis la page d'avis Glassdoor
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Note moyenne de l'entreprise ou 'NULL' si non trouvé
    """
    try:
        # Essayer différents sélecteurs possibles
        selectors = [
            ('div', {'class': 'rating'}),
            ('div', {'class': re.compile('.*ratingNum.*')}),
            ('span', {'class': re.compile('.*rating.*')}),
        ]
        
        for tag, attrs in selectors:
            element = objet_html.find(tag, attrs=attrs)
            if element and element.string:
                # Nettoyer et convertir la note
                note = element.string.strip()
                # Vérifier si c'est un nombre valide
                try:
                    float(note)
                    return note
                except ValueError:
                    continue
        
        # Si aucun sélecteur n'a fonctionné, chercher dans le texte avec regex
        text_with_rating = objet_html.find(text=re.compile(r'\d\.\d'))
        if text_with_rating:
            match = re.search(r'(\d\.\d)', text_with_rating)
            if match:
                return match.group(1)
                
        return 'NULL'
    except Exception as e:
        print(f"Erreur lors de l'extraction de la note moyenne: {str(e)}")
        return 'NULL'


#======================================================================================
#-- GLASSDOOR (SOC) : Fonctions renvoyant nom de l'entreprise, ville, taille, secteur
#======================================================================================

def extraire_nom_entreprise_SOC(objet_html):
    """
    Extrait le nom de l'entreprise depuis la page d'informations Glassdoor
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Nom de l'entreprise ou 'NULL' si non trouvé
    """
    texte_tmp = objet_html.find_all('h1', attrs = {'class':"strong tightAll"})[0].span.contents[0]
    if (texte_tmp == []) :
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp)
        resultat = re.sub(r'(.*)<h1 class=" strong tightAll" data-company="(.*)" title="">(.*)',
        r'\2', texte_tmp)
    return(resultat)

def extraire_ville_entreprise_SOC(objet_html):
    """
    Extrait la ville de l'entreprise depuis la page d'informations Glassdoor
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Ville de l'entreprise ou 'NULL' si non trouvé
    """
    texte_tmp = str(objet_html.find_all('div', attrs = {'class':"infoEntity"})[1].span.contents[0])
    if (texte_tmp == []) :
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp)
        texte_tmp_1 = re.sub(r'(.*)<h1 class=" strong tightAll" data-company="(.*)" title="">(.*)',
        r'\2', texte_tmp)
        resultat = texte_tmp_1
    return(resultat)

def extraire_taille_entreprise_SOC(objet_html):
    """
    Extrait la taille de l'entreprise depuis la page d'informations Glassdoor
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Taille de l'entreprise ou 'NULL' si non trouvé
    """
    texte_tmp = str(objet_html.find_all('div', attrs = {'class':"infoEntity"})[2].span.contents[0])
    if (texte_tmp == []) :
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp)
        texte_tmp_1 = re.sub(r'(.*)<h1 class=" strong tightAll" data-company="(.*)" title="">(.*)',
        r'\2', texte_tmp)
        resultat = texte_tmp_1
    return(resultat)

def extraire_secteur_entreprise_SOC(objet_html):
    """
    Extrait le secteur de l'entreprise depuis la page d'informations Glassdoor
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Taille de l'entreprise ou 'NULL' si non trouvé
    """
    texte_tmp = str(objet_html.find_all('div', attrs = {'class':"infoEntity"})[5].span.contents[0])
    if (texte_tmp == []) :
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp)
        texte_tmp_1 = re.sub(r'(.*)<h1 class=" strong tightAll" data-company="(.*)" title="">(.*)',
        r'\2', texte_tmp)
        resultat = texte_tmp_1
    return(resultat)

#==============================================================================
#-- LINKEDIN (EMP) : Fonctions renvoyant nom de l'entreprise, ville, taille
#==============================================================================
def extraire_libelle_emploi_EMP(objet_html):
    """
    Extrait le libellé de l'offre d'emploi depuis la page LinkedIn
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Libellé de l'offre d'emploi ou 'NULL' si non trouvé"""
    texte_tmp = objet_html.find_all('h1', attrs = {'class':'topcard__title'}) 
    if (texte_tmp == []) : 
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp[0].text)
        if (texte_tmp == []) : 
            resultat = 'NULL'
        else:
            resultat = texte_tmp
    return(resultat)

def extraire_nom_entreprise_EMP(objet_html):
    """
    Extrait le nom de l'entreprise depuis la page LinkedIn
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Nom de l'entreprise ou 'NULL' si non trouvé
    """

    texte_tmp = objet_html.find_all('span', attrs = {'class':'topcard__flavor'}) 
    if (texte_tmp == []) : 
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp[0].text)
        if (texte_tmp == []) : 
            resultat = 'NULL'
        else:
            resultat = texte_tmp
    return(resultat)

def extraire_ville_emploi_EMP (objet_html):
    """
    Extrait la ville de l'offre d'emploi depuis la page LinkedIn
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Ville de l'offre d'emploi ou 'NULL' si non trouvé
    """
    texte_tmp = objet_html.find_all('span', attrs = {'class':'topcard__flavor topcard__flavor--bullet'}) 
    if (texte_tmp == []) : 
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp[0].text)
        if (texte_tmp == []) : 
            resultat = 'NULL'
        else:
            resultat = texte_tmp
    return(resultat)

def extraire_texte_emploi_EMP (objet_html):
    """
    Extrait le texte de l'offre d'emploi depuis la page LinkedIn
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Texte de l'offre d'emploi ou 'NULL' si non trouvé
    """
    texte_tmp = objet_html.find_all('div', attrs = {"description__text description__text--rich"})
    if (texte_tmp == []) : 
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp[0].text)
        if (texte_tmp == []) : 
            resultat = 'NULL'
        else:
            resultat = texte_tmp
    return(resultat)

def extraire_niveau_hierarchique_emploi_EMP (objet_html):
    """
    Extrait le niveau hiérarchique de l'offre d'emploi depuis la page LinkedIn
    Args:
        objet_html: Objet BeautifulSoup contenant le HTML de la page
    Returns:
        str: Niveau hiérarchique de l'offre d'emploi ou 'NULL' si non trouvé
    """
    texte_tmp = objet_html.find_all('span', attrs = {'class':"job-criteria__text job-criteria__text--criteria"})
    if (texte_tmp == []) : 
        resultat = 'NULL'
    else:
        texte_tmp = str(texte_tmp[0].text)
        if (texte_tmp == []) : 
            resultat = 'NULL'
        else:
            resultat = texte_tmp
    return(resultat)

"""
Utilisation des fonctions d'extraction pour lire les fichiers HTML dans la curated zone
"""

# Lecture des métadonnées techniques pour obtenir la liste des fichiers cibles
metadonnees_techniques = "./DATALAKE/00_METADATA/metadata_technique.csv"

# Chargement des métadonnées techniques dans un DataFrame pandas pour récupérer les fichiers cibles
df_metadata_techniques= pd.read_csv(metadonnees_techniques, sep=';', encoding='utf-8')
df_metadata_techniques = df_metadata_techniques[df_metadata_techniques['colonne']=='fichier_cible']

# Initialisation des listes pour stocker les chemins des fichiers HTML
fichiers_glassdoor_societe_info = []
fichiers_linkedin_emp_info = []
fichiers_glassdoor_societe_avis = []

#Pour chaque fichier cible listé dans les métadonnées techniques lire le contenu HTML
for index, row in df_metadata_techniques.iterrows():
    chemin_du_fichier_html = row['valeur']

    # CONSTRUIRE 3 listes en fonction du type des fichiers INFO-SOC, AVIS-SOC, INFO-EMP
    if fnmatch.fnmatch(chemin_du_fichier_html, "*INFO-SOC-GLASSDOOR*.html"):
        fichiers_glassdoor_societe_info.append(chemin_du_fichier_html)
    elif fnmatch.fnmatch(chemin_du_fichier_html, "*AVIS-SOC-GLASSDOOR*.html"):
        fichiers_glassdoor_societe_avis.append(chemin_du_fichier_html)
    elif fnmatch.fnmatch(chemin_du_fichier_html, "*INFO-EMP-LINKEDIN*.html"):
        fichiers_linkedin_emp_info.append(chemin_du_fichier_html)

    objet_fichier_html = open(chemin_du_fichier_html, "r", encoding="utf8")
    texte_source_html = objet_fichier_html.read()
    objet_fichier_html.close()



############################################################################
# Parcours des fichiers HTML d'informations sur les sociétés sur Glassdoor
############################################################################

liste_entreprise = []
liste_ville_entreprise = []
liste_taille_entreprise = []
liste_secteur_entreprise = []

for fichier_html in fichiers_glassdoor_societe_info:
    # Ouvrir le fichier HTML
    objet_fichier_html = open(fichier_html, "r", encoding="utf8")
    texte_source_html = objet_fichier_html.read()
    objet_fichier_html.close()

    # Lire les fichiers HTML
    soup = BeautifulSoup(texte_source_html, 'html.parser')

    # Extraction des noms des entreprises
    nom_entreprise = extraire_nom_entreprise_SOC(soup)
    liste_entreprise.append(nom_entreprise)

    #extraction des villes des entreprises
    ville_entreprise = extraire_ville_entreprise_SOC(soup)
    liste_ville_entreprise.append(ville_entreprise)

    #extraction des tailles des entreprises
    taille_entreprise = extraire_taille_entreprise_SOC(soup)
    liste_taille_entreprise.append(taille_entreprise)

    #extraction des secteurs des entreprises
    secteur_entreprise = extraire_secteur_entreprise_SOC(soup)
    liste_secteur_entreprise.append(secteur_entreprise)

print("Nom de l'entreprise extraite : ", liste_entreprise)
print("Ville de l'entreprise extraite : ", liste_ville_entreprise)
print("Taille de l'entreprise extraite : ", liste_taille_entreprise)
print("Secteur de l'entreprise extraite : ", liste_secteur_entreprise)


############################################################################
# Parcours des fichiers HTML d'informations sur les avis sur Glassdoor
############################################################################
liste_entreprise_avis = []
liste_note_moy_entreprise = []

for fichier_html in fichiers_glassdoor_societe_avis:
    # Ouvrir le fichier HTML
    objet_fichier_html = open(fichier_html, "r", encoding="utf8")
    texte_source_html = objet_fichier_html.read()
    objet_fichier_html.close()

    # Lire les fichiers HTML
    soup = BeautifulSoup(texte_source_html, 'html.parser')

    # Extraction des noms des entreprises
    nom_entreprise_avi = extraire_nom_entreprise_AVI(soup)
    liste_entreprise_avis.append(nom_entreprise_avi)
    note_moy_entreprise_avi = extraire_note_moy_entreprise_AVI(soup)
    liste_note_moy_entreprise.append(note_moy_entreprise_avi)

print("Nom de l'entreprise extraite (AVIS) : ", liste_entreprise_avis)
print("Note moyenne de l'entreprise extraite (AVIS) : ", liste_note_moy_entreprise)

#############################################################################
# Parcours des fichiers HTML d'informations sur les offres d'emplois LinkedIn
#############################################################################
liste_libelle_emploi = []
liste_entreprise_emp = []
liste_ville_emploi = []
liste_texte_emploi = []
liste_niveau_hierarchique_emploi = []

for fichier_html in fichiers_linkedin_emp_info:
    # Ouvrir le fichier HTML
    objet_fichier_html = open(fichier_html, "r", encoding="utf8")
    texte_source_html = objet_fichier_html.read()
    objet_fichier_html.close()

    # Lire les fichiers HTML
    soup = BeautifulSoup(texte_source_html, 'html.parser')

    # Extraction des libellés des emplois
    libelle_emploi = extraire_libelle_emploi_EMP(soup)
    liste_libelle_emploi.append(libelle_emploi)

    # Extraction des noms des entreprises
    nom_entreprise_emp = extraire_nom_entreprise_EMP(soup)
    liste_entreprise_emp.append(nom_entreprise_emp)

    # Extraction des villes des emplois
    ville_emploi = extraire_ville_emploi_EMP(soup)
    liste_ville_emploi.append(ville_emploi)

    # Extraction des textes des emplois
    texte_emploi = extraire_texte_emploi_EMP(soup)
    liste_texte_emploi.append(texte_emploi)

    # Extraction des niveaux hiérarchiques des emplois
    niveau_hierarchique_emploi = extraire_niveau_hierarchique_emploi_EMP(soup)
    liste_niveau_hierarchique_emploi.append(niveau_hierarchique_emploi)

print("Libellé des emplois extraits : ", liste_libelle_emploi)
print("Nom de l'entreprise extraite (EMP) : ", liste_entreprise_emp)
print("Ville des emplois extraits : ", liste_ville_emploi)
#print("Texte des emplois extraits : ", liste_texte_emploi)
print("Niveau hiérarchique des emplois extraits : ", liste_niveau_hierarchique_emploi)