# Ce script ETL filtre F_avis.csv : il supprime les lignes dont le contenu d'avis est vide.
from pathlib import Path
import sys
import pandas as pd

# Fonction pour vérifier si une chaîne est vide ou ne contient que des espaces
def est_vide(s):
    """
    Vérifie si une chaîne est vide ou ne contient que des espaces.
    Args:
        s (str): Chaîne à vérifier.
    Returns:
        bool: True si la chaîne est vide ou ne contient que des espaces, False sinon.
    """
    if s is None:
        return True
    t = str(s)
    if t.strip() == '':
        return True
    return False

# Configuration des chemins
RACINE = Path(__file__).resolve().parents[1]
REPERTOIRE_ENTREE = RACINE / 'data_globale'
REPERTOIRE_SORTIE = RACINE / 'data_globale_etl'
REPERTOIRE_SORTIE.mkdir(parents=True, exist_ok=True)

fichier_entree = REPERTOIRE_ENTREE / 'F_avis.csv'
fichier_sortie = REPERTOIRE_SORTIE / 'F_avis.csv'

if not fichier_entree.exists():
    raise FileNotFoundError(f"Fichier source introuvable: {fichier_entree}")

# Lire le fichier CSV d'entrée
df_avant = pd.read_csv(fichier_entree, dtype=str, encoding='utf-8', keep_default_na=False, on_bad_lines='skip')
nb_original = len(df_avant)

# Identifier la colonne de contenu d'avis
candidats_contenu = ['contenu_avis', 'contenu', 'texte', 'contenu_avis_fr']
colonne_contenu = None
for c in candidats_contenu:
    if c in df_avant.columns:
        colonne_contenu = c
        break
# Si aucune colonne standard n'est trouvée, chercher une colonne contenant des mots-clés
if colonne_contenu is None:
    for c in df_avant.columns:
        lc = c.lower()
        if 'conten' in lc or 'avis' in lc or 'texte' in lc:
            colonne_contenu = c
            break
# Si toujours aucune colonne trouvée, afficher un message et sortir
if colonne_contenu is None:
    print('Aucune colonne de contenu trouvée dans F_avis.csv. Aucune ligne supprimée. Colonnes:', list(df_avant.columns))
    df_avant.to_csv(fichier_sortie, index=False, encoding='utf-8')
    print(f'Ecrit: {fichier_sortie} (inchangé)')
    sys.exit(0)


# Filtrer les lignes où la colonne de contenu d'avis est vide
masque_vide = df_avant[colonne_contenu].map(est_vide)
nb_vides = int(masque_vide.sum())
# Créer le DataFrame nettoyé
df_nettoye = df_avant[~masque_vide].reset_index(drop=True)
nb_ecrits = len(df_nettoye)

# Écrire le DataFrame nettoyé dans le fichier de sortie
df_nettoye.to_csv(fichier_sortie, index=False, encoding='utf-8')

# Afficher le résumé
print(f'Lu: {fichier_entree} lignes={nb_original}')
print(f'Lignes supprimées avec "{colonne_contenu}" vide: {nb_vides}')
print(f'Ecrit: {fichier_sortie} lignes={nb_ecrits}')
