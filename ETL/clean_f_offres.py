# Ce script nettoie le fichier F_offres.csv : normalise les dates, nettoie les textes et remplace les valeurs vides par 'NULL'.
import csv
import re
from pathlib import Path
import pandas as pd

# Fonctions de nettoyage
def normaliser_texte(s: str) -> str:
    if pd.isna(s):
        return ''
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    if s.startswith('"') and s.endswith('"'):
        s = s[1:-1]
    s = s.replace('\\n', ' ')
    s = s.replace('\n', ' ')
    s = re.sub(r'^[-•\s]+', '', s)
    s = s.replace(',', ' ')
    s = re.sub(r'\s+', ' ', s).strip()
    return s

# Normalisation des dates au format jj/mm/aa
def normaliser_date_iso_vers_jjmmaa(s: str) -> str:
    if pd.isna(s) or s == '':
        return 'NULL'
    s = str(s).strip()
    try:
        dt = pd.to_datetime(s, errors='coerce')
        if pd.isna(dt):
            return 'NULL'
        return dt.strftime('%d/%m/%Y')
    except Exception:
        return 'NULL'

# Fonction principale 
def principal():
    # Configuration des chemins
    racine_repo = Path(__file__).resolve().parent.parent
    chemin_src = racine_repo / 'data_globale' / 'F_offres.csv'
    dossier_sortie = racine_repo / 'data_globale_etl'
    dossier_sortie.mkdir(parents=True, exist_ok=True)
    chemin_dest = dossier_sortie / 'F_offres.csv'
    sauvegarde = dossier_sortie / 'F_offres.bak.csv'

    if not chemin_src.exists():
        print(f"Fichier source introuvable: {chemin_src}")
        return
    
    # Lecture du fichier source
    df_offres = pd.read_csv(chemin_src, dtype=str, keep_default_na=False)

    # Nettoyage des colonnes texte
    colonnes_texte = [c for c in df_offres.columns if c.lower() in ('libelle_emploi', 'contenu', 'description', 'libelle')]
    for col in colonnes_texte:
        print(f"Nettoyage de la colonne texte: {col}")
        df_offres[col] = df_offres[col].apply(normaliser_texte)

    if 'date_posted' in df_offres.columns:
        print('Normalisation de date_posted...')
        df_offres['date_posted'] = df_offres['date_posted'].apply(normaliser_date_iso_vers_jjmmaa)
    elif 'date' in df_offres.columns:
        print('Normalisation de date...')
        df_offres['date'] = df_offres['date'].apply(normaliser_date_iso_vers_jjmmaa)

    for col in df_offres.columns:
        if col == 'id_offre':
            continue
        df_offres[col] = df_offres[col].apply(lambda x: 'NULL' if (isinstance(x, str) and x.strip() == '') else x)

    if chemin_dest.exists():
        chemin_dest.rename(sauvegarde)
        print(f'Sortie existante sauvegardée vers: {sauvegarde}')

    # Ecriture du fichier nettoyé
    df_offres.to_csv(chemin_dest, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f'Ecriture du fichier nettoyé vers: {chemin_dest}')

# Lancer le script
if __name__ == '__main__':
    principal()
