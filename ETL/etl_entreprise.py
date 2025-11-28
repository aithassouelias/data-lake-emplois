# Ce script ETL nettoie et normalise d_entreprise.csv et produit d_entreprise.csv et d_secteur.csv dans data_globale_etl
from pathlib import Path
import re
import csv
import pandas as pd

def nettoyer_id_secteur(val):
    """
    Nettoie la valeur de id_secteur en supprimant les espaces et les suffixes .0
    Args:
        val (str): Valeur à nettoyer.
    Returns:
        str: Valeur nettoyée."""
    if pd.isna(val) or str(val).strip() == '':
        return ''
    s = str(val).strip()
    if s.endswith('.0'):
        s = s[:-2]
    s = re.sub(r"\.0$", "", s)
    return s

def est_ligne_uniquement_id(row):
    """
    Vérifie si une ligne contient uniquement des informations d'identifiant (id_entreprise, id_secteur)
    Args:
        row (pd.Series): Ligne du DataFrame.
    Returns:
        bool: True si la ligne contient uniquement des identifiants, False sinon."""
    nom = str(row.get('nom_entreprise','')).strip()
    taille = str(row.get('taille','')).strip()
    idsec = str(row.get('id_secteur','')).strip()
    if (not nom) and (not taille) and (not idsec):
        return True
    return False


# Configuration des chemins
RACINE = Path(__file__).resolve().parents[1]
REPERTOIRE_ENTREE = RACINE / 'data_globale'
REPERTOIRE_SORTIE = RACINE / 'data_globale_etl'
REPERTOIRE_SORTIE.mkdir(parents=True, exist_ok=True)

fichier_entreprise = REPERTOIRE_ENTREE / 'd_entreprise.csv'
fichier_secteur = REPERTOIRE_ENTREE / 'd_secteur.csv'
fichier_sortie_entreprise = REPERTOIRE_SORTIE / 'd_entreprise.csv'
fichier_sortie_secteur = REPERTOIRE_SORTIE / 'd_secteur.csv'

# Vérifier l'existence du fichier source
if not fichier_entreprise.exists():
    raise FileNotFoundError(f"Fichier source introuvable: {fichier_entreprise}")

# Lire d_entreprise.csv
df_entreprise = pd.read_csv(fichier_entreprise, dtype=str, encoding='utf-8', keep_default_na=False)
df_entreprise.columns = [c.strip() for c in df_entreprise.columns]


# Nettoyer la colonne id_secteur
if 'id_secteur' not in df_entreprise.columns:
    df_entreprise['id_secteur'] = ''
df_entreprise['id_secteur'] = df_entreprise['id_secteur'].apply(nettoyer_id_secteur)


# Supprimer les lignes qui ne contiennent que des identifiants
masque_seulement_id = df_entreprise.apply(est_ligne_uniquement_id, axis=1)
nb_suppr = masque_seulement_id.sum()
if nb_suppr > 0:
    df_entreprise = df_entreprise[~masque_seulement_id].reset_index(drop=True)

# Remplir les valeurs manquantes de id_secteur avec '42', représentant 'sans secteur'
masque_idsec_manquant = df_entreprise['id_secteur'].map(lambda x: str(x).strip() == '')
if masque_idsec_manquant.any():
    df_entreprise.loc[masque_idsec_manquant, 'id_secteur'] = '42'

# Normaliser la colonne taille et créer la colonne categorie
def remplir_taille(x):
    """
    Remplit la valeur de taille avec 'Inconnu' si elle est manquante ou vide.
    Args:
        x (str): Valeur de taille.
    Returns:
        str: Valeur de taille remplie.
    """
    if pd.isna(x) or str(x).strip() == '':
        return 'Inconnu'
    return str(x).strip()

# Remplir les valeurs manquantes de taille
if 'taille' not in df_entreprise.columns:
    df_entreprise['taille'] = 'Inconnu'
else:
    df_entreprise['taille'] = df_entreprise['taille'].apply(remplir_taille)

def extraire_nombres(s):
    """
    Extrait les nombres entiers d'une chaîne, en gérant les espaces insécables.
    Args:
        s (str): Chaîne d'entrée.
    Returns:    list: Liste des nombres entiers extraits."""
    if s is None:
        return []
    nums = re.findall(r"\d{1,3}(?:[ \u00A0]\d{3})*", s)
    out = []
    for n in nums:
        n_clean = n.replace(' ', '').replace('\u00A0','')
        try:
            out.append(int(n_clean))
        except Exception:
            continue
    return out

def categorie_depuis_taille(s):
    """
    Détermine la catégorie d'entreprise à partir de la taille.
    Args:
        s (str): Valeur de taille.
    Returns:
        str: Catégorie d'entreprise."""
    s = '' if s is None else str(s)
    s_low = s.lower()
    if s_low.strip() == '' or s_low.strip() == 'inconnu':
        return 'Inconnu'
    if 'plus de' in s_low:
        return 'Grande entreprise'
    nums = extraire_nombres(s_low)
    if not nums:
        if any(k in s_low for k in ['petite', 'pm', 'tpe']):
            return 'PME'
        return 'Autre'
    max_n = max(nums)
    if max_n >= 1000:
        return 'Grande entreprise'
    if max_n >= 51:
        return 'PME'
    if max_n >= 1:
        return 'PME'
    return 'Autre'

# Appliquer la fonction pour créer la colonne categorie
df_entreprise['categorie'] = df_entreprise['taille'].map(categorie_depuis_taille)

def normaliser_libelle_taille(s):
    """
    Normalise les libellés de taille en supprimant les espaces superflus.  
    Args:
        s (str): Libellé de taille.
    Returns:
        str: Libellé normalisé.
    """
    if s is None:
        return ''
    ns = re.sub(r"\s+", " ", str(s)).strip()
    return ns

# Dictionnaire de correspondance des tailles vers catégories
taille_vers_categorie = {
    normaliser_libelle_taille('De 1 à 50 employés'): 'TPE',
    normaliser_libelle_taille('De 51 à 200 employés'): 'PME',
    normaliser_libelle_taille('Entre 201 et 500 employés'): 'ETI',
    normaliser_libelle_taille('De 501 à 1 000 employés'): 'Moyenne entreprise',
    normaliser_libelle_taille('De 1 001 à 5 000 employés'): 'Grande entreprise',
    normaliser_libelle_taille('De 5 001 à 10 000 employés'): 'Très grande entreprise',
    normaliser_libelle_taille('Plus de 10 000 employés'): 'Multinationale',
    normaliser_libelle_taille('1946'): 'Autre',
    normaliser_libelle_taille('Inconnu'): 'Inconnu',
}

# Mapper les tailles vers catégories en utilisant le dictionnaire
def mapper_taille_vers_categorie(val):
    """
    Mappe une valeur de taille vers une catégorie en utilisant le dictionnaire de correspondance.
    Args:
        val (str): Valeur de taille.
    Returns:
        str: Catégorie correspondante."""
    if val is None:
        return 'Inconnu'
    key = normaliser_libelle_taille(val)
    return taille_vers_categorie.get(key, 'Autre')

# Appliquer le mapping pour mettre à jour la colonne categorie
df_entreprise['categorie'] = df_entreprise['taille'].map(mapper_taille_vers_categorie)
# Réorganiser les colonnes et s'assurer qu'elles existent toutes
colonnes_finales = ['id_entreprise','id_secteur','nom_entreprise','taille','categorie']
for c in colonnes_finales:
    if c not in df_entreprise.columns:
        df_entreprise[c] = ''
# Réorganiser les colonnes
df_entreprise = df_entreprise[colonnes_finales]

# Traiter d_secteur.csv
if fichier_secteur.exists():
    df_secteur = pd.read_csv(fichier_secteur, dtype=str, encoding='utf-8', keep_default_na=False)
    df_secteur.columns = [c.strip() for c in df_secteur.columns]
else:
    df_secteur = pd.DataFrame(columns=['id_secteur','secteur'])

if 'id_secteur' in df_secteur.columns:
    df_secteur['id_secteur'] = df_secteur['id_secteur'].map(lambda x: '' if pd.isna(x) else str(x).strip())
    df_secteur['id_secteur'] = df_secteur['id_secteur'].apply(lambda v: re.sub(r"\.0$", "", v) if v else v)
else:
    df_secteur['id_secteur'] = ''
    df_secteur['secteur'] = ''

existe_42 = any(str(x).strip() == '42' for x in df_secteur['id_secteur'].tolist())
if not existe_42:
    df_secteur = pd.concat([df_secteur, pd.DataFrame([{'id_secteur':'42','secteur':'sans secteur'}])], ignore_index=True)

colonnes_dsec = ['id_secteur','secteur']
for c in colonnes_dsec:
    if c not in df_secteur.columns:
        df_secteur[c] = ''

# Réorganiser les colonnes de d_secteur
df_secteur = df_secteur[colonnes_dsec]
df_secteur.to_csv(fichier_sortie_secteur, index=False, encoding='utf-8')
# Sauvegarder d_entreprise.csv nettoyé
df_entreprise.to_csv(fichier_sortie_entreprise, index=False, encoding='utf-8')

# Afficher le résumé
print('ETL terminé.')
print(f'Ecrit: {fichier_sortie_entreprise}')
print(f'Ecrit: {fichier_sortie_secteur}')
print(f'Supprimé {nb_suppr} lignes qui contenaient seulement id_entreprise')
