# Ce script lit le metadata descriptif et génère les CSV de la zone data_globale (d_ville,d_secteur,d_entreprise,d_type_poste,d_note,F_offres,F_avis).
from pathlib import Path
import sys
import json
import pandas as pd
import numpy as np

# Configuration des chemins
RACINE = Path(__file__).resolve().parents[1]
CHEMIN_META = RACINE / 'DATALAKE' / '00_METADATA' / 'metadata_descriptives.csv'
DOSSIER_SORTIE = RACINE / 'data_globale'
DOSSIER_SORTIE.mkdir(parents=True, exist_ok=True)

# Lire le fichier metadata_descriptives.csv
print(f"Lecture du metadata depuis {CHEMIN_META}")
lignes = []
with open(CHEMIN_META, 'r', encoding='utf-8', errors='replace') as f:
    header = f.readline()
    for ln in f:
        if not ln.strip():
            continue
        parts = ln.rstrip('\n').split(';', 3)
        if len(parts) < 4:
            parts += [''] * (4 - len(parts))
        object_id, type_fichier, colonne, valeur = parts
        lignes.append({'OBJECT_ID': object_id.strip(), 'TYPE_FICHIER': type_fichier.strip(), 'colonne': colonne.strip(), 'valeur': valeur.strip()})
df_meta = pd.DataFrame(lignes)

# Pivoter les données
df_meta.columns = [c.strip() for c in df_meta.columns]
pivot = df_meta.copy()
pivot['valeur'] = pivot['valeur'].astype(str).replace({'None':'', 'nan':''})
tableau_large = pivot.groupby(['OBJECT_ID','TYPE_FICHIER','colonne'], as_index=False)['valeur']\
    .agg(lambda s: ' '.join([x for x in s if x and x != 'nan']))\
    .pivot_table(index=['OBJECT_ID','TYPE_FICHIER'], columns='colonne', values='valeur', aggfunc='first')\
    .reset_index()

tableau_large.columns.name = None
tableau_large = tableau_large.rename_axis(None, axis=1)

# Fonction pour créer les id
def creer_table_id(series, col_name, id_name):
    s = series.dropna().map(lambda x: x.strip()).replace('', np.nan).dropna().drop_duplicates().reset_index(drop=True)
    out = pd.DataFrame({col_name: s})
    out[id_name] = range(1, len(out) + 1)
    out = out[[id_name, col_name]]
    return out

# créer les tables de dimension en df pandas
if 'ville' in tableau_large.columns:
    d_ville = creer_table_id(tableau_large['ville'], 'ville', 'id_ville')
else:
    d_ville = pd.DataFrame(columns=['id_ville','ville'])

if 'secteur' in tableau_large.columns:
    d_secteur = creer_table_id(tableau_large['secteur'], 'secteur', 'id_secteur')
else:
    d_secteur = pd.DataFrame(columns=['id_secteur','secteur'])

type_values = []
if 'niveau_hierarchique' in tableau_large.columns:
    type_values = tableau_large['niveau_hierarchique'].dropna().map(lambda x: x.strip()).replace('', np.nan).dropna().unique().tolist()
if not type_values and 'libelle_emploi' in tableau_large.columns:
    lib = tableau_large['libelle_emploi'].dropna().map(str)
    extracted = lib.map(lambda x: x.split()[:2] if x else []).map(lambda parts: ' '.join(parts) if parts else None)
    type_values = extracted.dropna().map(lambda x: x.strip()).replace('', np.nan).dropna().unique().tolist()

if type_values:
    d_type_poste = pd.DataFrame({'type_poste': type_values})
    d_type_poste['id_type_poste'] = range(1, len(d_type_poste) + 1)
    d_type_poste = d_type_poste[['id_type_poste','type_poste']]
else:
    d_type_poste = pd.DataFrame(columns=['id_type_poste','type_poste'])

parts_note = []
for c in ['note_moy_entreprise', 'note']:
    if c in tableau_large.columns:
        parts_note.append(tableau_large[c].dropna())

avis_parse = []
if 'avis' in tableau_large.columns:
    for _, r in tableau_large[['OBJECT_ID'] + [c for c in tableau_large.columns if c in ['nom_entreprise','entreprise','taille','avis']]].iterrows():
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
                    advantage = v.get('avantages') or v.get('avantage') or ''
                    inconvenient = v.get('inconvenients') or v.get('inconvenient') or v.get('inconvienet') or ''
                    avis_parse.append({'OBJECT_ID': r['OBJECT_ID'], 'date_avis': date_avis, 'note_avis': note_avis, 'texte_avis': texte_avis, 'avantage': advantage, 'inconvenient': inconvenient, 'nom_entreprise': r.get('nom_entreprise') or r.get('entreprise'), 'taille': r.get('taille')})
        except Exception:
            continue

if parts_note:
    note_series = pd.concat(parts_note, ignore_index=True)
else:
    note_series = pd.Series(dtype=str)
note_series = note_series.map(lambda x: x.strip()).replace('', np.nan).dropna()

if avis_parse:
    note_vals_from_avis = []
    for a in avis_parse:
        na = a.get('note_avis')
        if na is not None and str(na).strip():
            note_vals_from_avis.append(str(na).strip())
    if note_vals_from_avis:
        note_series = pd.concat([note_series, pd.Series(note_vals_from_avis)], ignore_index=True)

def en_float_sur(x):
    try:
        return float(str(x).replace(',', '.'))
    except Exception:
        return None
################################################
# Génération de la table d_note
###############################################

note_vals = note_series.map(en_float_sur).dropna().drop_duplicates().reset_index(drop=True)
if not note_vals.empty:
    d_note = pd.DataFrame({'note': note_vals})
    d_note['id_note'] = range(1, len(d_note) + 1)
    d_note = d_note[['id_note','note']]
else:
    d_note = pd.DataFrame(columns=['id_note','note'])

################################################
# Génération de la table d_entreprise
################################################
ent_cols = []
for c in ['nom_entreprise','entreprise']:
    if c in tableau_large.columns:
        ent_cols.append(c)
if 'nom_entreprise' in tableau_large.columns:
    source_entreprise = tableau_large[['nom_entreprise','taille','secteur']].rename(columns={'nom_entreprise':'nom_entreprise'})
else:
    source_entreprise = pd.DataFrame(columns=['nom_entreprise','taille','secteur'])

if not source_entreprise.empty:
    source_entreprise['nom_entreprise'] = source_entreprise['nom_entreprise'].map(lambda x: x.strip() if pd.notna(x) else x)
    source_entreprise['taille'] = source_entreprise['taille'].map(lambda x: x.strip() if pd.notna(x) else x)
    source_entreprise['secteur'] = source_entreprise['secteur'].map(lambda x: x.strip() if pd.notna(x) else x)
    entreprises_uniques = source_entreprise.drop_duplicates(subset=['nom_entreprise','taille','secteur']).reset_index(drop=True)
    entreprises_uniques['id_entreprise'] = range(1, len(entreprises_uniques)+1)
    if not d_secteur.empty:
        entreprises_uniques = entreprises_uniques.merge(d_secteur, how='left', left_on='secteur', right_on='secteur')
        entreprises_uniques = entreprises_uniques.rename(columns={'id_secteur':'id_secteur'})
    else:
        entreprises_uniques['id_secteur'] = pd.NA
    d_entreprise = entreprises_uniques[['id_entreprise','id_secteur','nom_entreprise','taille']].rename(columns={'nom_entreprise':'nom_entreprise','taille':'taille'})
    d_entreprise = d_entreprise.rename(columns={'nom_entreprise':'nom_entreprise','taille':'taille'})
else:
    d_entreprise = pd.DataFrame(columns=['id_entreprise','id_secteur','nom_entreprise','taille'])

# créer des mappings pour les ids
ville_vers_id = dict(zip(d_ville['ville'], d_ville['id_ville'])) if not d_ville.empty else {}
secteur_vers_id = dict(zip(d_secteur['secteur'], d_secteur['id_secteur'])) if not d_secteur.empty else {}
entreprise_vers_id = {}
if not d_entreprise.empty:
    for _,row in d_entreprise.iterrows():
        key = (row['nom_entreprise'], row['taille'])
        entreprise_vers_id[key] = row['id_entreprise']

type_vers_id = dict(zip(d_type_poste['type_poste'], d_type_poste['id_type_poste'])) if not d_type_poste.empty else {}
note_vers_id = dict()
if not d_note.empty:
    note_vers_id = dict(zip(d_note['note'], d_note['id_note']))

###############################################################
# Génération de la table F_offres
################################################################
offres = []
offer_rows = tableau_large[tableau_large.get('libelle_emploi').notna() | tableau_large.get('texte').notna()]
next_offre_id = 1
for _,r in offer_rows.iterrows():
    id_offre = next_offre_id
    next_offre_id += 1
    nom = r.get('nom_entreprise') if 'nom_entreprise' in r.index else None
    taille = r.get('taille') if 'taille' in r.index else None
    ent_id = entreprise_vers_id.get((nom, taille)) if (nom is not None and taille is not None) else None
    if not ent_id and nom:
        for (ename, etaille), eid in entreprise_vers_id.items():
            if ename == nom:
                ent_id = eid
                break
    ville = r.get('ville') if 'ville' in r.index else None
    ville_id = ville_vers_id.get(ville) if ville else pd.NA
    tp = r.get('niveau_hierarchique') if 'niveau_hierarchique' in r.index else None
    if not tp:
        lib = r.get('libelle_emploi') if 'libelle_emploi' in r.index else None
        if pd.notna(lib):
            tp_candidate = ' '.join(str(lib).split()[:2])
            if tp_candidate in type_vers_id:
                tp = tp_candidate
    tp_id = type_vers_id.get(tp, pd.NA) if tp else pd.NA
    libelle = r.get('libelle_emploi') if 'libelle_emploi' in r.index else ''
    contenu = r.get('texte') if 'texte' in r.index else ''
    date_posted = r.get('date_posted') if 'date_posted' in r.index else pd.NA
    offres.append({'id_offre': id_offre, 'id_entreprise': ent_id if ent_id else pd.NA, 'id_ville': ville_id, 'id_type_poste': tp_id, 'libelle_emploi': libelle, 'contenu': contenu, 'date_posted': date_posted})

F_offres = pd.DataFrame(offres)

#############################################################
# Génération de la table F_avis
#############################################################
liste_avis = []
next_avis_id = 1

for a in avis_parse:
    id_avis = next_avis_id
    next_avis_id += 1
    note_val = en_float_sur(a.get('note_avis')) if a.get('note_avis') is not None else None
    id_note = note_vers_id.get(note_val, pd.NA) if note_val is not None else pd.NA
    date_pub = a.get('date_avis') if a.get('date_avis') else pd.NA
    contenu_avis = a.get('texte_avis') or ''
    inconvenient = a.get('inconvenient') or ''
    avantage = a.get('avantage') or ''
    nom = a.get('nom_entreprise')
    taille = a.get('taille')
    ent_id = pd.NA
    if nom:
        ent_id = entreprise_vers_id.get((nom, taille)) if (nom is not None and taille is not None) else None
        if not ent_id:
            for (ename, etaille), eid in entreprise_vers_id.items():
                if ename and ename.strip().lower() == str(nom).strip().lower():
                    ent_id = eid
                    break
    liste_avis.append({'id_avis': id_avis, 'id_note': id_note, 'date_publication': date_pub, 'contenu_avis': contenu_avis, 'inconvenient': inconvenient, 'avantage': avantage, 'id_entreprise': ent_id if ent_id else pd.NA})

avis_rows = tableau_large[tableau_large.get('avis').notna() | tableau_large.get('note_moy_entreprise').notna() | tableau_large.get('date_posted').notna()]
for _,r in avis_rows.iterrows():
    obj = r.get('OBJECT_ID')
    parsed_from_obj = any(str(a.get('OBJECT_ID')) == str(obj) for a in avis_parse)
    if parsed_from_obj:
        pass
    note_val = None
    for c in ['note_moy_entreprise','note']:
        if c in r.index and pd.notna(r[c]) and r[c] != '':
            note_val = en_float_sur(r[c])
            break
    id_note = note_vers_id.get(note_val, pd.NA) if note_val is not None else pd.NA
    date_pub = r.get('date_posted') if 'date_posted' in r.index else pd.NA
    contenu_avis = ''
    if not parsed_from_obj:
        contenu_avis = r.get('avis') if 'avis' in r.index else ''
    inconvenient = r.get('inconvienet') if 'inconvienet' in r.index else (r.get('inconvenient') if 'inconvenient' in r.index else '')
    avantage = r.get('avantage') if 'avantage' in r.index else ''
    nom = r.get('nom_entreprise') if 'nom_entreprise' in r.index else (r.get('entreprise') if 'entreprise' in r.index else None)
    taille = r.get('taille') if 'taille' in r.index else None
    ent_id = pd.NA
    if nom:
        ent_id = entreprise_vers_id.get((nom, taille)) if (nom is not None and taille is not None) else None
        if not ent_id:
            for (ename, etaille), eid in entreprise_vers_id.items():
                if ename and ename.strip().lower() == str(nom).strip().lower():
                    ent_id = eid
                    break
    if (id_note is not pd.NA) or (contenu_avis and str(contenu_avis).strip()) or (date_pub and str(date_pub).strip()):
        id_avis = next_avis_id
        next_avis_id += 1
        liste_avis.append({'id_avis': id_avis, 'id_note': id_note, 'date_publication': date_pub, 'contenu_avis': contenu_avis, 'inconvenient': inconvenient, 'avantage': avantage, 'id_entreprise': ent_id if ent_id else pd.NA})

F_avis = pd.DataFrame(liste_avis)

###############################################################
# Écriture des fichiers CSV pour les différentes tables
###############################################################
print('Ecriture des CSV vers', DOSSIER_SORTIE)

d_ville.to_csv(DOSSIER_SORTIE / 'd_ville.csv', index=False, encoding='utf-8')
d_secteur.to_csv(DOSSIER_SORTIE / 'd_secteur.csv', index=False, encoding='utf-8')
d_entreprise.to_csv(DOSSIER_SORTIE / 'd_entreprise.csv', index=False, encoding='utf-8')
d_type_poste.to_csv(DOSSIER_SORTIE / 'd_type_poste.csv', index=False, encoding='utf-8')
d_note.to_csv(DOSSIER_SORTIE / 'd_note.csv', index=False, encoding='utf-8')
F_offres.to_csv(DOSSIER_SORTIE / 'F_offres.csv', index=False, encoding='utf-8')
F_avis.to_csv(DOSSIER_SORTIE / 'F_avis.csv', index=False, encoding='utf-8')

print('Terminé. Fichiers créés:')
for p in DOSSIER_SORTIE.iterdir():
    if p.is_file():
        print(' -', p.name)
