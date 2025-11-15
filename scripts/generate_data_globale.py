"""
Script: generate_data_globale.py
But: lit le fichier DATALAKE/00_METADATA/metadata_descriptives.csv (format long : OBJECT_ID;TYPE_FICHIER;colonne;valeur)
Produit un dossier `data_globale/` contenant un CSV pour chaque table du schéma :
 - d_ville.csv
 - d_secteur.csv
 - d_entreprise.csv
 - d_type_poste.csv
 - d_note.csv
 - F_offres.csv
 - F_avis.csv

Le script crée des clés primaires séquentielles (1..n) et remplit les clés étrangères en fonction des correspondances.
Usage:
    python scripts/generate_data_globale.py

Dépendances: pandas (si non installé, installez-le: pip install pandas)
"""

from pathlib import Path
import sys
import json

try:
    import pandas as pd
    import numpy as np
except Exception as e:
    print("This script requires pandas and numpy. Install with: pip install pandas numpy")
    raise

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'DATALAKE' / '00_METADATA' / 'metadata_descriptives.csv'
OUT_DIR = ROOT / 'data_globale'
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Read file manually to preserve semicolons inside the `valeur` field (split only on first 3 ';')
print(f"Reading metadata from {SRC}")
rows = []
with open(SRC, 'r', encoding='utf-8', errors='replace') as f:
    header = f.readline()
    for ln in f:
        if not ln.strip():
            continue
        parts = ln.rstrip('\n').split(';', 3)
        if len(parts) < 4:
            parts += [''] * (4 - len(parts))
        object_id, type_fichier, colonne, valeur = parts
        rows.append({'OBJECT_ID': object_id.strip(), 'TYPE_FICHIER': type_fichier.strip(), 'colonne': colonne.strip(), 'valeur': valeur.strip()})
df = pd.DataFrame(rows)

# Normalize column names
df.columns = [c.strip() for c in df.columns]
# Pivot to wide: one row per OBJECT_ID
pivot = df.copy()
# coerce valeur to str and collapse multiple into single string per OBJECT_ID/colonne
pivot['valeur'] = pivot['valeur'].astype(str).replace({'None':'', 'nan':''})
wide = pivot.groupby(['OBJECT_ID','TYPE_FICHIER','colonne'], as_index=False)['valeur']\
    .agg(lambda s: ' '.join([x for x in s if x and x != 'nan']))\
    .pivot_table(index=['OBJECT_ID','TYPE_FICHIER'], columns='colonne', values='valeur', aggfunc='first')\
    .reset_index()

# flatten columns
wide.columns.name = None
wide = wide.rename_axis(None, axis=1)

# Helper to create id mapping dataframe
def make_id_df(series, col_name, id_name):
    s = series.dropna().map(lambda x: x.strip()).replace('', np.nan).dropna().drop_duplicates().reset_index(drop=True)
    out = pd.DataFrame({col_name: s})
    out[id_name] = range(1, len(out) + 1)
    # reorder
    out = out[[id_name, col_name]]
    return out

# d_ville
if 'ville' in wide.columns:
    d_ville = make_id_df(wide['ville'], 'ville', 'id_ville')
else:
    d_ville = pd.DataFrame(columns=['id_ville','ville'])

# d_secteur
if 'secteur' in wide.columns:
    d_secteur = make_id_df(wide['secteur'], 'secteur', 'id_secteur')
else:
    d_secteur = pd.DataFrame(columns=['id_secteur','secteur'])

# d_type_poste: prefer 'niveau_hierarchique' else try to extract from libelle_emploi
type_values = []
if 'niveau_hierarchique' in wide.columns:
    type_values = wide['niveau_hierarchique'].dropna().map(lambda x: x.strip()).replace('', np.nan).dropna().unique().tolist()
# fallback: take first word of libelle_emploi if niveau missing
if not type_values and 'libelle_emploi' in wide.columns:
    lib = wide['libelle_emploi'].dropna().map(str)
    extracted = lib.map(lambda x: x.split()[:2] if x else []).map(lambda parts: ' '.join(parts) if parts else None)
    type_values = extracted.dropna().map(lambda x: x.strip()).replace('', np.nan).dropna().unique().tolist()

if type_values:
    d_type_poste = pd.DataFrame({'type_poste': type_values})
    d_type_poste['id_type_poste'] = range(1, len(d_type_poste) + 1)
    d_type_poste = d_type_poste[['id_type_poste','type_poste']]
else:
    d_type_poste = pd.DataFrame(columns=['id_type_poste','type_poste'])

# d_note: collect notes from note_moy_entreprise, note and from individual parsed avis
note_parts = []
for c in ['note_moy_entreprise', 'note']:
    if c in wide.columns:
        note_parts.append(wide[c].dropna())

# Parse JSON 'avis' fields (some rows store multiple avis as JSON blobs)
avis_parsed = []
if 'avis' in wide.columns:
    for _, r in wide[['OBJECT_ID'] + [c for c in wide.columns if c in ['nom_entreprise','entreprise','taille','avis']]].iterrows():
        avis_val = r.get('avis')
        if pd.isna(avis_val) or not str(avis_val).strip():
            continue
        s = str(avis_val).strip()
        try:
            j = json.loads(s)
            if isinstance(j, dict):
                for k, v in j.items():
                    if not isinstance(v, dict):
                        continue
                    date_avis = v.get('date_avis') or v.get('date')
                    note_avis = v.get('note_avis') or v.get('note')
                    texte_avis = v.get('texte_avis') or v.get('texte') or ''
                    avantage = v.get('avantages') or v.get('avantage') or ''
                    inconvenient = v.get('inconvenients') or v.get('inconvenient') or v.get('inconvienet') or ''
                    avis_parsed.append({'OBJECT_ID': r['OBJECT_ID'], 'date_avis': date_avis, 'note_avis': note_avis, 'texte_avis': texte_avis, 'avantage': avantage, 'inconvenient': inconvenient, 'nom_entreprise': r.get('nom_entreprise') or r.get('entreprise'), 'taille': r.get('taille')})
        except Exception:
            # not JSON - ignore for parsing, will be treated as raw avis text below
            continue

if note_parts:
    note_series = pd.concat(note_parts, ignore_index=True)
else:
    note_series = pd.Series(dtype=str)
note_series = note_series.map(lambda x: x.strip()).replace('', np.nan).dropna()

# include note_avis from parsed avis
if avis_parsed:
    note_vals_from_avis = []
    for a in avis_parsed:
        na = a.get('note_avis')
        if na is not None and str(na).strip():
            note_vals_from_avis.append(str(na).strip())
    if note_vals_from_avis:
        note_series = pd.concat([note_series, pd.Series(note_vals_from_avis)], ignore_index=True)

# try to convert to float
def to_float_safe(x):
    try:
        return float(str(x).replace(',', '.'))
    except Exception:
        return None

note_vals = note_series.map(to_float_safe).dropna().drop_duplicates().reset_index(drop=True)
if not note_vals.empty:
    d_note = pd.DataFrame({'note': note_vals})
    d_note['id_note'] = range(1, len(d_note) + 1)
    d_note = d_note[['id_note','note']]
else:
    d_note = pd.DataFrame(columns=['id_note','note'])

# d_entreprise: group by nom_entreprise, taille, secteur
ent_cols = []
for c in ['nom_entreprise','entreprise']:
    if c in wide.columns:
        ent_cols.append(c)
# prefer nom_entreprise
if 'nom_entreprise' in wide.columns:
    ent_source = wide[['nom_entreprise','taille','secteur']].rename(columns={'nom_entreprise':'nom_entreprise'})
else:
    ent_source = pd.DataFrame(columns=['nom_entreprise','taille','secteur'])

# normalize
if not ent_source.empty:
    ent_source['nom_entreprise'] = ent_source['nom_entreprise'].map(lambda x: x.strip() if pd.notna(x) else x)
    ent_source['taille'] = ent_source['taille'].map(lambda x: x.strip() if pd.notna(x) else x)
    ent_source['secteur'] = ent_source['secteur'].map(lambda x: x.strip() if pd.notna(x) else x)
    ent_unique = ent_source.drop_duplicates(subset=['nom_entreprise','taille','secteur']).reset_index(drop=True)
    ent_unique['id_entreprise'] = range(1, len(ent_unique)+1)
    # add id_secteur FK by merging with d_secteur
    if not d_secteur.empty:
        ent_unique = ent_unique.merge(d_secteur, how='left', left_on='secteur', right_on='secteur')
        ent_unique = ent_unique.rename(columns={'id_secteur':'id_secteur'})
    else:
        ent_unique['id_secteur'] = pd.NA
    d_entreprise = ent_unique[['id_entreprise','id_secteur','nom_entreprise','taille']].rename(columns={'nom_entreprise':'nom_entreprise','taille':'taille'})
    d_entreprise = d_entreprise.rename(columns={'nom_entreprise':'nom_entreprise','taille':'taille'})
else:
    d_entreprise = pd.DataFrame(columns=['id_entreprise','id_secteur','nom_entreprise','taille'])

# Prepare mapping lookups
ville_to_id = dict(zip(d_ville['ville'], d_ville['id_ville'])) if not d_ville.empty else {}
secteur_to_id = dict(zip(d_secteur['secteur'], d_secteur['id_secteur'])) if not d_secteur.empty else {}
entreprise_to_id = {}
if not d_entreprise.empty:
    # map by nom_entreprise (and taille/secteur) fallback to first match
    for _,row in d_entreprise.iterrows():
        key = (row['nom_entreprise'], row['taille'])
        entreprise_to_id[key] = row['id_entreprise']

type_to_id = dict(zip(d_type_poste['type_poste'], d_type_poste['id_type_poste'])) if not d_type_poste.empty else {}
note_to_id = dict()
if not d_note.empty:
    note_to_id = dict(zip(d_note['note'], d_note['id_note']))

# F_offres
offers = []
offer_rows = wide[wide.get('libelle_emploi').notna() | wide.get('texte').notna()]
next_offre_id = 1
for _,r in offer_rows.iterrows():
    id_offre = next_offre_id
    next_offre_id += 1
    # id_entreprise: try match nom_entreprise or entreprise
    nom = r.get('nom_entreprise') if 'nom_entreprise' in r.index else None
    taille = r.get('taille') if 'taille' in r.index else None
    ent_id = entreprise_to_id.get((nom, taille)) if (nom is not None and taille is not None) else None
    # fallback try by nom only
    if not ent_id and nom:
        for (ename, etaille), eid in entreprise_to_id.items():
            if ename == nom:
                ent_id = eid
                break
    # ville
    ville = r.get('ville') if 'ville' in r.index else None
    ville_id = ville_to_id.get(ville) if ville else pd.NA
    # type_poste
    tp = r.get('niveau_hierarchique') if 'niveau_hierarchique' in r.index else None
    if not tp:
        # try match by first two words of libelle_emploi
        lib = r.get('libelle_emploi') if 'libelle_emploi' in r.index else None
        if pd.notna(lib):
            tp_candidate = ' '.join(str(lib).split()[:2])
            if tp_candidate in type_to_id:
                tp = tp_candidate
    tp_id = type_to_id.get(tp, pd.NA) if tp else pd.NA
    libelle = r.get('libelle_emploi') if 'libelle_emploi' in r.index else ''
    contenu = r.get('texte') if 'texte' in r.index else ''
    # date_posted related to the offer (if present)
    date_posted = r.get('date_posted') if 'date_posted' in r.index else pd.NA
    offers.append({'id_offre': id_offre, 'id_entreprise': ent_id if ent_id else pd.NA, 'id_ville': ville_id, 'id_type_poste': tp_id, 'libelle_emploi': libelle, 'contenu': contenu, 'date_posted': date_posted})

F_offres = pd.DataFrame(offers)

# F_avis
avis_list = []
next_avis_id = 1

# First, add avis parsed from JSON blobs (one row per individual review)
for a in avis_parsed:
    id_avis = next_avis_id
    next_avis_id += 1
    # note
    note_val = to_float_safe(a.get('note_avis')) if a.get('note_avis') is not None else None
    id_note = note_to_id.get(note_val, pd.NA) if note_val is not None else pd.NA
    # date
    date_pub = a.get('date_avis') if a.get('date_avis') else pd.NA
    contenu_avis = a.get('texte_avis') or ''
    inconvenient = a.get('inconvenient') or ''
    avantage = a.get('avantage') or ''
    # id_entreprise: try to match using nom_entreprise or entreprise
    nom = a.get('nom_entreprise')
    taille = a.get('taille')
    ent_id = pd.NA
    if nom:
        # direct match
        ent_id = entreprise_to_id.get((nom, taille)) if (nom is not None and taille is not None) else None
        if not ent_id:
            for (ename, etaille), eid in entreprise_to_id.items():
                if ename and ename.strip().lower() == str(nom).strip().lower():
                    ent_id = eid
                    break
    avis_list.append({'id_avis': id_avis, 'id_note': id_note, 'date_publication': date_pub, 'contenu_avis': contenu_avis, 'inconvenient': inconvenient, 'avantage': avantage, 'id_entreprise': ent_id if ent_id else pd.NA})

# Then, add rows from wide that represent single avis or general company-level notes
avis_rows = wide[wide.get('avis').notna() | wide.get('note_moy_entreprise').notna() | wide.get('date_posted').notna()]
for _,r in avis_rows.iterrows():
    # if this OBJECT_ID already had JSON-parsed avis, skip to avoid duplicates
    obj = r.get('OBJECT_ID')
    # check if any parsed avis came from this OBJECT_ID
    parsed_from_obj = any(str(a.get('OBJECT_ID')) == str(obj) for a in avis_parsed)
    if parsed_from_obj:
        # still add company-level note if present (note_moy_entreprise) but skip raw 'avis' content
        pass
    # note (company level)
    note_val = None
    for c in ['note_moy_entreprise','note']:
        if c in r.index and pd.notna(r[c]) and r[c] != '':
            note_val = to_float_safe(r[c])
            break
    id_note = note_to_id.get(note_val, pd.NA) if note_val is not None else pd.NA
    # date
    date_pub = r.get('date_posted') if 'date_posted' in r.index else pd.NA
    # contenu_avis: if parsed JSON existed for this OBJECT_ID, skip raw avis text to avoid duplication
    contenu_avis = ''
    if not parsed_from_obj:
        contenu_avis = r.get('avis') if 'avis' in r.index else ''
    # inconvenient / avantage if present
    inconvenient = r.get('inconvienet') if 'inconvienet' in r.index else (r.get('inconvenient') if 'inconvenient' in r.index else '')
    avantage = r.get('avantage') if 'avantage' in r.index else ''
    # id_entreprise
    nom = r.get('nom_entreprise') if 'nom_entreprise' in r.index else (r.get('entreprise') if 'entreprise' in r.index else None)
    taille = r.get('taille') if 'taille' in r.index else None
    ent_id = pd.NA
    if nom:
        ent_id = entreprise_to_id.get((nom, taille)) if (nom is not None and taille is not None) else None
        if not ent_id:
            for (ename, etaille), eid in entreprise_to_id.items():
                if ename and ename.strip().lower() == str(nom).strip().lower():
                    ent_id = eid
                    break
    # If we have either a note or a contenu_avis or date, create a row
    if (id_note is not pd.NA) or (contenu_avis and str(contenu_avis).strip()) or (date_pub and str(date_pub).strip()):
        id_avis = next_avis_id
        next_avis_id += 1
        avis_list.append({'id_avis': id_avis, 'id_note': id_note, 'date_publication': date_pub, 'contenu_avis': contenu_avis, 'inconvenient': inconvenient, 'avantage': avantage, 'id_entreprise': ent_id if ent_id else pd.NA})

F_avis = pd.DataFrame(avis_list)

# Write out CSVs
print('Writing CSV files to', OUT_DIR)

d_ville.to_csv(OUT_DIR / 'd_ville.csv', index=False, encoding='utf-8')
d_secteur.to_csv(OUT_DIR / 'd_secteur.csv', index=False, encoding='utf-8')
d_entreprise.to_csv(OUT_DIR / 'd_entreprise.csv', index=False, encoding='utf-8')
d_type_poste.to_csv(OUT_DIR / 'd_type_poste.csv', index=False, encoding='utf-8')
d_note.to_csv(OUT_DIR / 'd_note.csv', index=False, encoding='utf-8')
F_offres.to_csv(OUT_DIR / 'F_offres.csv', index=False, encoding='utf-8')
F_avis.to_csv(OUT_DIR / 'F_avis.csv', index=False, encoding='utf-8')

print('Done. Files created:')
for p in OUT_DIR.iterdir():
    if p.is_file():
        print(' -', p.name)

print('\nNotes:')
print(' - Les colonnes manquantes dans le fichier metadata seront laissées vides dans les CSV (valeurs NULL).')
print(' - Vérifiez les correspondances entreprise<->secteur/ville avant chargement en base si besoin.')
