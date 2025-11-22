import pandas as pd
import string
import nltk
from nltk.corpus import stopwords

# Télécharger les stopwords FR si pas déjà fait
nltk.download("stopwords")

# Stopwords français
stopwords_fr = set(stopwords.words("french"))
stopwords_custom = {"l'", "il y a", "telques", "assez", "être", "chez"}

stopwords_fr = stopwords_fr.union(stopwords_custom)


def nettoyer_texte(text):
    if pd.isna(text):
        return ""

    # Tout en minuscules
    text = text.lower()

    # Remplacer les expressions multi-mots AVANT tokenisation
    text = text.replace("il y a", " ")  
    text = text.replace("l'", " ")  

    # Enlever la ponctuation
    text = text.translate(str.maketrans("", "", string.punctuation))

    # Enlever les chiffres
    text = ''.join([c for c in text if not c.isdigit()])

    # Tokeniser
    mots = text.split()

    # Stopwords personnalisés
    custom_stopwords = {"telques", "telque"}

    # Combiner avec les stopwords français
    full_stopwords = stopwords_fr.union(custom_stopwords)

    # Retirer tous les stopwords
    mots = [m for m in mots if m not in full_stopwords]

    # Recomposer
    return " ".join(mots)



# Charger ton CSV
df = pd.read_csv("./data_globale_etl/F_avis.csv")

# Nettoyer les colonnes
df["avantage"] = df["avantage"].apply(nettoyer_texte)
df["inconvenient"] = df["inconvenient"].apply(nettoyer_texte)

# Aperçu
print(df.columns)

df.to_csv("F_avis.csv", index=False, encoding="utf-8")

