# Ce script nettoie le fichier F_avis.csv : normalise les dates, nettoie les textes et remplace les valeurs vides par 'NULL'.
from pathlib import Path
import pandas as pd
from dateutil import parser
import re

RACINE = Path(__file__).resolve().parents[1]
CHEMIN_F_AVIS = RACINE / 'data_globale_etl' / 'F_avis.csv'

if not CHEMIN_F_AVIS.exists():
    raise FileNotFoundError(f"Fichier manquant: {CHEMIN_F_AVIS}")

print('Lecture de', CHEMIN_F_AVIS)
df_avis = pd.read_csv(CHEMIN_F_AVIS, dtype=str, encoding='utf-8', keep_default_na=False)

def nettoyer_texte(s: str) -> str:
    if s is None:
        return ''
    s = str(s)
    s = s.replace('"', '')
    s = s.replace('\\n', ' ').replace('\n', ' ')
    s = s.replace('\\t', ' ').replace('\t', ' ')
    s = re.sub(r"^\s*[-–—]+\s*", '', s)
    s = s.replace(',', ' ')
    s = re.sub(r"\s+", ' ', s).strip()
    return s

def formater_date(s: str) -> str:
    s = nettoyer_texte(s)
    if not s:
        return ''
    try:
        dt = parser.parse(s, dayfirst=False, fuzzy=True)
        return dt.strftime('%d/%m/%Y')
    except Exception:
        try:
            dt = parser.parse(s, dayfirst=True, fuzzy=True)
            return dt.strftime('%d/%m/%Y')
        except Exception:
            return s

colonnes = list(df_avis.columns)

if 'date_publication' in df_avis.columns:
    print('Normalisation de date_publication...')
    df_avis['date_publication'] = df_avis['date_publication'].apply(formater_date)
else:
    print('Colonne date_publication introuvable')

colonnes_texte = ['contenu_avis', 'inconvenient', 'avantage']
for col in colonnes_texte:
    if col in df_avis.columns:
        print(f'Nettoyage de la colonne texte: {col}')
        df_avis[col] = df_avis[col].apply(nettoyer_texte)

for col in df_avis.columns:
    if col in ['id_avis', 'id_note', 'id_entreprise', 'date_publication']:
        continue
    if df_avis[col].dtype == object:
        df_avis[col] = df_avis[col].apply(lambda x: nettoyer_texte(x))

colonnes_a_traiter = [c for c in df_avis.columns if c != 'id_avis']
if colonnes_a_traiter:
    df_avis[colonnes_a_traiter] = df_avis[colonnes_a_traiter].replace(r'^\s*$', 'NULL', regex=True)

sauvegarde = CHEMIN_F_AVIS.with_suffix('.bak.csv')
print('Ecriture de la sauvegarde vers', sauvegarde)
df_avis.to_csv(sauvegarde, index=False, encoding='utf-8')
print('Ecriture finale vers', CHEMIN_F_AVIS)
df_avis.to_csv(CHEMIN_F_AVIS, index=False, encoding='utf-8')
print('Terminé. Lignes :', len(df_avis))
