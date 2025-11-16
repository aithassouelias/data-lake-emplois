#!/usr/bin/env python3
"""
Script pour détecter les entreprises aux noms proches (fuzzy matching) dans
`data_globale_etl/d_entreprise.csv` et afficher les paires (id, nom) similaires.

Usage:
  python trouver_entreprises_proches.py --seuil 0.85

Le script utilise la librairie standard `difflib` (SequenceMatcher) pour le score.

"""
from pathlib import Path
import argparse
import pandas as pd
import difflib
import re
import csv


def normaliser_chaine(s: str) -> str:
    if s is None:
        return ''
    s2 = str(s).strip().lower()
    # retirer ponctuation basique et espaces multiples
    s2 = re.sub(r"[\W_]+", ' ', s2)
    s2 = re.sub(r"\s+", ' ', s2).strip()
    return s2


def charger_entreprises(chemin_csv: Path) -> pd.DataFrame:
    if not chemin_csv.exists():
        raise FileNotFoundError(f"Fichier introuvable: {chemin_csv}")
    df = pd.read_csv(chemin_csv, dtype=str, encoding='utf-8', keep_default_na=False)
    if 'id_entreprise' not in df.columns or 'nom_entreprise' not in df.columns:
        raise RuntimeError('Le fichier doit contenir les colonnes id_entreprise et nom_entreprise')
    # garder toutes les colonnes pour calculer le nombre d'informations manquantes
    df = df.copy()
    df['nom_normalise'] = df['nom_entreprise'].map(normaliser_chaine)
    # calculer un score d'information: nombre de colonnes non 'Inconnu' et non vides
    def compte_inconnus(row):
        vals = [str(v).strip() for v in row.tolist()]
        # ignorer id et nom
        vals = vals[2:]
        return sum(1 for v in vals if (v == '' or v.lower() == 'inconnu' or v.lower() == 'none' or v.lower() == 'nan'))

    df['nb_inconnu'] = df.apply(lambda r: compte_inconnus(r), axis=1)
    return df


def trouver_paires_proches(df: pd.DataFrame, seuil: float = 0.85):
    """Retourne une liste de tuples (score, idx_i, idx_j) pour paires similaires (indices dans df)."""
    lignes = []
    n = len(df)
    normes = df['nom_normalise'].tolist()

    for i in range(n):
        ni = normes[i]
        if not ni:
            continue
        for j in range(i + 1, n):
            nj = normes[j]
            if not nj:
                continue
            score = difflib.SequenceMatcher(None, ni, nj).ratio()
            if score >= seuil:
                lignes.append((score, i, j))

    lignes.sort(key=lambda x: x[0], reverse=True)
    return lignes


def regrouper_composantes(n_pairs, n):
 
    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    for _, i, j in n_pairs:
        union(i, j)

    comps = {}
    for idx in range(n):
        r = find(idx)
        comps.setdefault(r, []).append(idx)
    # ne garder que les composantes de taille>1
    clusters = [sorted(v) for v in comps.values() if len(v) > 1]
    return clusters


def choisir_representant(df: pd.DataFrame, indices: list):
    """Choisir l'indice à garder: celui avec le plus d'infos (moins de 'Inconnu')."""
    sous = df.iloc[indices].copy()
    # nb_inconnu plus petit -> meilleur
    min_inconnu = sous['nb_inconnu'].min()
    candidats = sous[sous['nb_inconnu'] == min_inconnu]
    if len(candidats) == 1:
        return int(candidats.index[0])
    # en cas d'égalité, choisir l'id_entreprise le plus petit (int)
    def id_int(idx):
        try:
            return int(df.at[idx, 'id_entreprise'])
        except Exception:
            return float('inf')

    best = min(candidats.index.tolist(), key=id_int)
    return int(best)


def main():
    parser = argparse.ArgumentParser(description='Trouver entreprises aux noms proches (fuzzy)')
    parser.add_argument('--seuil', type=float, default=0.85, help='Seuil de similarité (0-1), défaut 0.85')
    parser.add_argument('--fichier', type=str, default='data_globale_etl/d_entreprise.csv', help='Chemin vers d_entreprise.csv')
    parser.add_argument('--sortie', type=str, default='', help='Si précisé: écrit les paires trouvées dans ce CSV')
    args = parser.parse_args()

    chemin = Path(args.fichier)
    print(f'Lecture de : {chemin}')
    df = charger_entreprises(chemin)
    print(f'Enregistrements lus: {len(df)}')

    print(f'Détection des paires avec seuil = {args.seuil}...')
    paires = trouver_paires_proches(df, seuil=args.seuil)

    if not paires:
        print('Aucune paire similaire trouvée avec ce seuil.')
        return

    print(f'Trouvé {len(paires)} paire(s) similaire(s) (seuil={args.seuil}).')
    # regrouper en clusters
    clusters = regrouper_composantes(paires, len(df))
    print(f'Composantes détectées (clusters) : {len(clusters)}')

    actions = []  # tuples (cluster_indices, kept_idx, removed_indices)
    for comp in clusters:
        kept = choisir_representant(df, comp)
        removed = [i for i in comp if i != kept]
        actions.append((comp, kept, removed))

    # Affichage: pour chaque cluster, afficher les lignes similaires et indiquer le gardé
    for comp, kept, removed in actions:
        print('\nCluster:')
        for idx in comp:
            print(f' - {df.at[idx, "id_entreprise"]}  ,  {df.at[idx, "nom_entreprise"]}')
        print(f' -> Gardé: {df.at[kept, "id_entreprise"]} , {df.at[kept, "nom_entreprise"]}')

    # préparer DataFrame dédupliqué
    indices_a_supprimer = [idx for _, _, removed in actions for idx in removed]
    df_dedupe = df.drop(index=indices_a_supprimer).reset_index(drop=True)

    # écriture du CSV dédupliqué si demandé
    chemin_sortie_defaut = Path('data_globale_etl/d_entreprise_deduplique.csv')
    if args.sortie:
        chemin_sortie = Path(args.sortie)
    else:
        chemin_sortie = chemin_sortie_defaut

    print(f'Nombre de lignes supprimées: {len(indices_a_supprimer)}')
    print(f'Ecriture du fichier dédupliqué vers: {chemin_sortie}')
    # écrire toutes les colonnes d'origine sauf les lignes supprimées
    df_dedupe.to_csv(chemin_sortie, index=False, encoding='utf-8')

    print('\nTerminé.')


if __name__ == '__main__':
    main()
