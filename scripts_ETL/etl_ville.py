# Ce script ETL nettoie et normalise d_ville.csv et produit d_ville.csv dans data_globale_etl
from pathlib import Path
import re
import unicodedata
import pandas as pd

RACINE = Path(__file__).resolve().parents[1]
REPERTOIRE_ENTREE = RACINE / 'data_globale'
REPERTOIRE_SORTIE = RACINE / 'data_globale_etl'
REPERTOIRE_SORTIE.mkdir(parents=True, exist_ok=True)

fichier_ville = REPERTOIRE_ENTREE / 'd_ville.csv'
fichier_sortie_ville = REPERTOIRE_SORTIE / 'd_ville.csv'

if not fichier_ville.exists():
    raise FileNotFoundError(f"Fichier source introuvable: {fichier_ville}")

def enlever_accents(s: str) -> str:
    if s is None:
        return ''
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join([c for c in nfkd if not unicodedata.combining(c)])

COUNTRY_KEYWORDS = {
    'etats-unis': 'États-Unis', 'etats unis': 'États-Unis', 'etatsunis': 'États-Unis',
    'royaume-uni': 'Royaume-Uni', 'angleterre': 'Royaume-Uni', 'uk': 'Royaume-Uni',
    'allemagne': 'Allemagne', 'suisse': 'Suisse', 'suede': 'Suède', 'suède': 'Suède',
    'italie': 'Italie', 'canada': 'Canada', 'maroc': 'Maroc', 'pologne': 'Pologne', 'russie': 'Russie',
    'hong kong': 'Hong Kong', 'chine': 'Chine', 'luxembourg': 'Luxembourg', 'belgique': 'Belgique',
    'espagne': 'Espagne', 'irlande': 'Irlande'
}

FRENCH_REGION_KEYWORDS = [
    'ile-de-france', 'auvergne', 'rhone', 'rhone-alpes', 'provence', 'aquitaine', 'nouvelle-aquitaine',
    'ile de france', 'auvergne-rhone', 'provence-alpes', 'hauts-de-seine', 'ile-de-france',
    'france', 'fr'
]

def normaliser_texte(s: str) -> str:
    if s is None:
        return ''
    s2 = str(s).strip()
    if len(s2) >= 2 and s2[0] == '"' and s2[-1] == '"':
        s2 = s2[1:-1]
    s2 = re.sub(r"\s+", " ", s2).strip()
    return s2

def detecter_pays_depuis_texte(text: str) -> str | None:
    if not text:
        return None
    t = enlever_accents(text.lower())
    for k, name in COUNTRY_KEYWORDS.items():
        if k in t:
            return name
    code = t.strip().upper()
    if code in ('FR', 'FRA'):
        return 'France'
    if code in ('US', 'USA', 'U.S.', 'U.S'):
        return 'États-Unis'
    if any(reg in t for reg in FRENCH_REGION_KEYWORDS):
        return 'France'
    return None

def parser_ville_pays(raw: str):
    s = normaliser_texte(raw)
    if not s:
        return ('', '')
    m = re.match(r'^(.*?)\s*\(([^)]+)\)\s*$', s)
    if m:
        city = m.group(1).strip()
        country_part = m.group(2).strip()
        detected = detecter_pays_depuis_texte(country_part)
        return (city, detected or country_part)
    if ',' in s:
        left, right = [p.strip() for p in s.split(',', 1)]
        detected = detecter_pays_depuis_texte(right)
        if detected:
            return (left, detected)
        if any(rk in enlever_accents(right.lower()) for rk in FRENCH_REGION_KEYWORDS):
            return (left, 'France')
        if re.fullmatch(r'[A-Za-z]{2,3}', right):
            if right.upper() in ('FR', 'FRA'):
                return (left, 'France')
            if right.upper() in ('US', 'USA'):
                return (left, 'États-Unis')
        return (left, right)
    return (s, 'France')

def executer():
    df = pd.read_csv(fichier_ville, dtype=str, encoding='utf-8', keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    if 'id_ville' not in df.columns:
        if len(df.columns) >= 2:
            df = df.rename(columns={df.columns[0]: 'id_ville', df.columns[1]: 'ville'})
        else:
            raise RuntimeError('Format inattendu de d_ville.csv')

    postal_map = {
        '75009': 'Paris',
        '69230': 'Saint-Genis-Laval',
    }

    def ressemble_pas_ville(text: str) -> bool:
        if not text:
            return True
        t = text.lower()
        if 'employ' in t or 'entre' in t and 'employ' in t:
            return True
        if t.strip().startswith(('de ', 'entre ', 'plus de')):
            return True
        if re.search(r'\d{1,3}(?:[ \u00A0]\d{3})*', t) and 'employ' in t:
            return True
        return False

    lignes_sortie = []
    for _, row in df.iterrows():
        vid = row.get('id_ville', '')
        raw = row.get('ville', '')
        city, country = parser_ville_pays(raw)
        if city and ',' in city:
            city = city.split(',', 1)[0].strip()
            city = re.sub(r"\s+\d{1,2}$", "", city)
            city = re.sub(r"\s+Area$", "", city, flags=re.IGNORECASE)
        else:
            if city:
                city = re.sub(r"\s+\d{1,2}$", "", city)
                city = re.sub(r"\s+Area$", "", city, flags=re.IGNORECASE)
        city_digits = re.match(r'^\s*(\d{5})\s*$', str(city))
        if city_digits:
            code = city_digits.group(1)
            mapped = postal_map.get(code)
            if mapped:
                city = mapped
                country = 'France'
            else:
                city = 'Inconnu'
                country = 'Inconnu'
        if ressemble_pas_ville(raw) or ressemble_pas_ville(city):
            city = 'Inconnu'
            country = 'Inconnu'
        if not city or str(city).strip() == '':
            city = 'Inconnu'
            country = 'Inconnu'
        if not country or str(country).strip() == '':
            country = 'France'
        lignes_sortie.append({'id_ville': vid, 'ville': city, 'pays': country or ''})

    df_sortie = pd.DataFrame(lignes_sortie)
    df_sortie.to_csv(fichier_sortie_ville, index=False, encoding='utf-8')
    print('Ecriture de :', fichier_sortie_ville)


if __name__ == '__main__':
    executer()
