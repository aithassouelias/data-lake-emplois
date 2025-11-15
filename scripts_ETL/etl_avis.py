# Ce script ETL filtre F_avis.csv : il supprime les lignes dont le contenu d'avis est vide.
from pathlib import Path
import sys
import pandas as pd

RACINE = Path(__file__).resolve().parents[1]
REPERTOIRE_ENTREE = RACINE / 'data_globale'
REPERTOIRE_SORTIE = RACINE / 'data_globale_etl'
REPERTOIRE_SORTIE.mkdir(parents=True, exist_ok=True)

fichier_entree = REPERTOIRE_ENTREE / 'F_avis.csv'
fichier_sortie = REPERTOIRE_SORTIE / 'F_avis.csv'

if not fichier_entree.exists():
    raise FileNotFoundError(f"Fichier source introuvable: {fichier_entree}")

df_avant = pd.read_csv(fichier_entree, dtype=str, encoding='utf-8', keep_default_na=False, on_bad_lines='skip')
nb_original = len(df_avant)

candidats_contenu = ['contenu_avis', 'contenu', 'texte', 'contenu_avis_fr']
colonne_contenu = None
for c in candidats_contenu:
    if c in df_avant.columns:
        colonne_contenu = c
        break

if colonne_contenu is None:
    for c in df_avant.columns:
        lc = c.lower()
        if 'conten' in lc or 'avis' in lc or 'texte' in lc:
            colonne_contenu = c
            break

if colonne_contenu is None:
    print('Aucune colonne de contenu trouvée dans F_avis.csv. Aucune ligne supprimée. Colonnes:', list(df_avant.columns))
    df_avant.to_csv(fichier_sortie, index=False, encoding='utf-8')
    print(f'Ecrit: {fichier_sortie} (inchangé)')
    sys.exit(0)

def est_vide(s):
    if s is None:
        return True
    t = str(s)
    if t.strip() == '':
        return True
    return False

masque_vide = df_avant[colonne_contenu].map(est_vide)
nb_vides = int(masque_vide.sum())
df_nettoye = df_avant[~masque_vide].reset_index(drop=True)
nb_ecrits = len(df_nettoye)

df_nettoye.to_csv(fichier_sortie, index=False, encoding='utf-8')

print(f'Lu: {fichier_entree} lignes={nb_original}')
print(f'Lignes supprimées avec "{colonne_contenu}" vide: {nb_vides}')
print(f'Ecrit: {fichier_sortie} lignes={nb_ecrits}')
