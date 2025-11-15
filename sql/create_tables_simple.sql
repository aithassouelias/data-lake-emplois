

-- sql/create_tables_simple.sql
-- Version PostgreSQL simple avec auto-incrément (SERIAL)

-- Table de ville
CREATE TABLE IF NOT EXISTS d_ville (
    id_ville SERIAL PRIMARY KEY,
    ville TEXT NOT NULL
);

-- Table des secteurs
CREATE TABLE IF NOT EXISTS d_secteur (
    id_secteur SERIAL PRIMARY KEY,
    secteur TEXT
);

-- Dimension entreprise
CREATE TABLE IF NOT EXISTS d_entreprise (
    id_entreprise SERIAL PRIMARY KEY,
    id_secteur INTEGER REFERENCES d_secteur(id_secteur),
    nom_entreprise TEXT NOT NULL,
    siege_social TEXT,
    taille TEXT
);

-- Dimension type de poste
CREATE TABLE IF NOT EXISTS d_type_poste (
    id_type_poste SERIAL PRIMARY KEY,
    type_poste TEXT NOT NULL
);

-- Dimension note
CREATE TABLE IF NOT EXISTS d_note (
    id_note SERIAL PRIMARY KEY,
    note REAL
);

-- Table des offres
CREATE TABLE IF NOT EXISTS f_offres (
    id_offre SERIAL PRIMARY KEY,
    id_entreprise INTEGER REFERENCES d_entreprise(id_entreprise),
    id_ville INTEGER REFERENCES d_ville(id_ville),
    id_type_poste INTEGER REFERENCES d_type_poste(id_type_poste),
    libelle_emploi TEXT,
    contenu TEXT
);

-- Table des avis (fact)
CREATE TABLE IF NOT EXISTS f_avis (
    id_avis SERIAL PRIMARY KEY,
    id_note INTEGER REFERENCES d_note(id_note),
    date_publication TIMESTAMP,
    contenu_avis TEXT,
    inconvienet TEXT,
    avantage TEXT,
    id_offre INTEGER REFERENCES f_offres(id_offre),
    -- lien direct vers l'entreprise (optionnel)
    id_entreprise INTEGER REFERENCES d_entreprise(id_entreprise)
);

-- Indexes simples pour accélérer les jointures
CREATE INDEX IF NOT EXISTS idx_offres_entreprise ON f_offres (id_entreprise);
CREATE INDEX IF NOT EXISTS idx_offres_ville ON f_offres (id_ville);
CREATE INDEX IF NOT EXISTS idx_avis_note ON f_avis (id_note);
CREATE INDEX IF NOT EXISTS idx_avis_offre ON f_avis (id_offre);
CREATE INDEX IF NOT EXISTS idx_avis_entreprise ON f_avis (id_entreprise);
