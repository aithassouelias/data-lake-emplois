# M2 BIA – Projet Data Lake pour l’Analyse du Marché de l’Emploi

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/BeautifulSoup-Web%20Scraping-FFD43B?logo=python&logoColor=black" />
  <img src="https://img.shields.io/badge/Pandas-Data%20Processing-150458?logo=pandas" />
  <img src="https://img.shields.io/badge/Power%20BI-Visualisation-F2C811?logo=powerbi&logoColor=black" />
</p>

---

## 📌 Description du projet

Ce projet a été réalisé dans le cadre du Master 2 Business Intelligence & Analytics.  
Il vise à construire un **pipeline de données complet** pour **analyser le marché de l’emploi**, en récupérant des informations issues d’offres d’emploi et d’avis d’entreprises provenant de différentes sources (Glassdoor et LinkedIn).

Le travail inclut :
- 🧭 la mise en place d’un **Data Lake** structuré en zones,  
- 🛠️ la création d’un **pipeline ETL en Python**,  
- 🧹 le nettoyage et l’enrichissement des données,  
- 🔍 des algorithmes de **matching** pour les différentes sources,  
- 🗄️ le chargement dans un **Data Warehouse**,  
- 📊 la création d’un **dashboard Power BI** permettant d’explorer les tendances du marché de l’emploi.

---

## 🏗️ Architecture du projet

### `datalake/`
Contient l’ensemble des zones du Data Lake, depuis les données sources jusqu’aux tables prêtes pour l’analyse.

- **00_FICHIER_METADATA/**  
  Regroupe les fichiers de métadonnées : dictionnaire des champs, description des sources, schéma du Data Lake.

- **0_SOURCE_DE_DONNEES/**  
  Données brutes collectées avant tout traitement (fichiers HTML).

- **1_LANDING_ZONE/**  
  Zone d’atterrissage contenant les données juste après extraction, dans leur format d’origine mais structurées en fonction de leur source et écritures des métadonnées         techniques.

- **2_CURATED_ZONE/**  
  Extraction des données utiles dans les fichiers et écritures des métadonnées descriptives.

- **3_PRODUCTION_ZONE/**  
  Zone finale contenant les tables de faits et dimensions du Data Warehouse, prêtes pour les analyses et tableaux de bord.


### `ETL/`
Scripts Python responsables de l’ensemble du pipeline ETL :
- nettoyage,
- standardisation,
- enrichissement (API, règles métier),
- génération des clés,
- chargement des tables dans le DW.

Inclut aussi des utilitaires : fonctions de nettoyage, appel API, classification, etc.


### `dataviz/`
Regroupe tout ce qui concerne la restitution :
- rapport Power BI (fichier `.pbip`),


---

## 🤝 Contributeurs

- Yousra Bouhanna
- Abdeldjebbar Abid
- Elias Ait Hassou
