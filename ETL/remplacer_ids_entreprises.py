#!/usr/bin/env python3
"""
Applique le mapping d'identifiants d'entreprises issu de la détection fuzzy
pour remplacer les ids supprimés par les ids gardés dans `F_avis.csv` et
`F_offres.csv`.

Usage:
  python remplacer_ids_entreprises.py --seuil 0.85

Le script importe les fonctions de `trouver_entreprises_proches.py` pour
reproduire les clusters et choisir le représentant, puis met à jour les
deux fichiers 
"""
from pathlib import Path
import argparse
import pandas as pd
import importlib.util


def _charger_module_trouver():
    """Charge dynamiquement le module trouver_entreprises_proches.py
    et retourne les fonctions nécessaires.
    """
    module_path = Path(__file__).parent / 'trouver_entreprises_proches.py'
    spec = importlib.util.spec_from_file_location('trouver_entreprises_proches', str(module_path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return (
        mod.charger_entreprises,
        mod.trouver_paires_proches,
        mod.regrouper_composantes,
        mod.choisir_representant,
    )


# charger les fonctions du module local
charger_entreprises, trouver_paires_proches, regrouper_composantes, choisir_representant = _charger_module_trouver()


def construire_mapping(chemin_entreprises: Path, seuil: float = 0.85):
    df = charger_entreprises(chemin_entreprises)
    paires = trouver_paires_proches(df, seuil=seuil)
    if not paires:
        return {}, df

    clusters = regrouper_composantes(paires, len(df))
    mapping = {}
    for comp in clusters:
        kept_idx = choisir_representant(df, comp)
        kept_id = str(df.at[kept_idx, 'id_entreprise'])
        for idx in comp:
            if idx == kept_idx:
                continue
            removed_id = str(df.at[idx, 'id_entreprise'])
            mapping[removed_id] = kept_id

    return mapping, df


def appliquer_mapping_sur_csv(chemin_csv: Path, colonne: str, mapping: dict, inplace: bool = False):
    if not chemin_csv.exists():
        print(f"Fichier introuvable, skip: {chemin_csv}")
        return 0

    df = pd.read_csv(chemin_csv, dtype=str, encoding='utf-8', keep_default_na=False)
    if colonne not in df.columns:
        print(f"Colonne '{colonne}' non trouvée dans {chemin_csv}. Fichier inchangé.")
        return 0

    # Appliquer mapping (les clés/valeurs sont des str)
    def mapper_val(v):
        if pd.isna(v):
            return v
        s = str(v)
        return mapping.get(s, s)

    avant = df[colonne].astype(str).tolist()
    df[colonne] = df[colonne].apply(mapper_val)
    apres = df[colonne].astype(str).tolist()

    # compter remplacements
    remplacements = sum(1 for a, b in zip(avant, apres) if a != b)

    if inplace:
        out = chemin_csv
    else:
        out = chemin_csv.with_name(chemin_csv.stem + '_updated' + chemin_csv.suffix)

    df.to_csv(out, index=False, encoding='utf-8')
    print(f'Ecrit {out}  (remplacements appliqués: {remplacements})')
    return remplacements


def main():
    parser = argparse.ArgumentParser(description='Remplacer ids d\'entreprise supprimés par ids gardés')
    parser.add_argument('--seuil', type=float, default=0.85, help='Seuil de similarité (0-1)')
    parser.add_argument('--entreprises', type=str, default='data_globale_etl/d_entreprise.csv', help='Chemin vers d_entreprise.csv')
    parser.add_argument('--f_avis', type=str, default='data_globale_etl/F_avis.csv', help='Chemin vers F_avis.csv')
    parser.add_argument('--f_offres', type=str, default='data_globale_etl/F_offres.csv', help='Chemin vers F_offres.csv')
    parser.add_argument('--inplace', action='store_true', help='Écraser les fichiers originaux (dangerous)')
    args = parser.parse_args()

    chemin_ent = Path(args.entreprises)
    print(f'Calcul du mapping (seuil={args.seuil}) depuis: {chemin_ent}')
    mapping, df_ent = construire_mapping(chemin_ent, seuil=args.seuil)

    if not mapping:
        print('Aucun mapping trouvé (pas de paires similaires). Rien à appliquer.')
        return

    print(f'Mapping trouvé: {len(mapping)} ids supprimés → id gardé (ex: {next(iter(mapping.items()))})')

    # Appliquer sur F_avis et F_offres
    total = 0
    total += appliquer_mapping_sur_csv(Path(args.f_avis), 'id_entreprise', mapping, inplace=args.inplace)
    total += appliquer_mapping_sur_csv(Path(args.f_offres), 'id_entreprise', mapping, inplace=args.inplace)

    print(f'Total remplacements appliqués: {total}')


if __name__ == '__main__':
    main()
