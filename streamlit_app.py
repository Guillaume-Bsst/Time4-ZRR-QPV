import os
import sys
import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import streamlit as st

# ================== CONFIG ==================

# ⚠ Tu peux garder ta clé ici pour des tests locaux,
#   mais en prod je te conseille de passer par st.secrets (cf. plus bas).
API_KEY_SIRENE = "baec8347-d5ad-4056-ac83-47d5ad10565e"

SIRENE_URL = "https://api.insee.fr/api-sirene/3.11/siret/{}"
BAN_URL = "https://api-adresse.data.gouv.fr/search/"

QPV_GEO_PATH = "QP2024_France_Hexagonale_Outre_Mer_WGS84.gpkg"
ZRR_CSV_PATH = "ZRR_list_source.csv"

# Colonnes QPV
COL_CODE_QP = "code_qp"
COL_LIB_QP = "lib_qp"
COL_LIB_COM = "lib_com"

# Colonne libellé ZRR (nom de la commune)
ZRR_LIB_COL = "LIBGEO"

# ================== CHARGEMENT DONNÉES (avec cache) ==================


@st.cache_resource
def load_qpv_polygones(path: str) -> gpd.GeoDataFrame:
    """
    Charge le fichier QPV et le projette en Lambert-93 (EPSG:2154)
    pour calculer des distances en mètres.
    """
    gdf = gpd.read_file(path)

    if gdf.crs is None:
        raise ValueError("Le fichier QPV n'a pas de système de coordonnées (CRS).")

    # On travaille en Lambert-93 pour la France (mètres)
    if gdf.crs.to_epsg() != 2154:
        gdf = gdf.to_crs(epsg=2154)

    return gdf


@st.cache_resource
def load_zrr_data(path: str):
    """
    Charge le CSV ZRR (avec header=5 comme dans ton code)
    et renvoie :
      - df_zrr nettoyé
      - l'ensemble des communes ZRR (totales ou partielles)
    """
    df_zrr = pd.read_csv(path, header=5)
    # Normalisation du code commune
    df_zrr["CODGEO"] = df_zrr["CODGEO"].astype(str).str.zfill(5)

    communes_zrr = set(
        df_zrr.loc[
            df_zrr["ZRR_SIMP"].str.startswith(("C", "P"), na=False),
            "CODGEO",
        ].tolist()
    )
    return df_zrr, communes_zrr


# ================== FONCTIONS METIER ==================


def get_sirene_etab(siret: str) -> dict:
    """Interroge l'API SIRENE 3.11 et renvoie l'objet 'etablissement'."""
    headers = {"X-INSEE-Api-Key-Integration": API_KEY_SIRENE}
    url = SIRENE_URL.format(siret)
    r = requests.get(url, headers=headers, timeout=10)
    if r.status_code != 200:
        raise RuntimeError(f"Erreur API SIRENE ({r.status_code}) : {r.text}")
    data = r.json()
    etab = data.get("etablissement")
    if etab is None:
        raise ValueError("Réponse SIRENE invalide : pas de champ 'etablissement'.")
    return etab


def adresse_depuis_sirene(etab: dict):
    """Construit adresse texte + CP + commune à partir de l'établissement SIRENE."""
    adr = etab.get("adresseEtablissement", {})

    numero = adr.get("numeroVoieEtablissement") or ""
    type_voie = adr.get("typeVoieEtablissement") or ""
    lib_voie = adr.get("libelleVoieEtablissement") or ""
    comp = adr.get("complementAdresseEtablissement") or ""

    code_postal = adr.get("codePostalEtablissement") or ""
    commune = adr.get("libelleCommuneEtablissement") or ""

    ligne1 = " ".join(x for x in [str(numero), type_voie, lib_voie] if x).strip()
    if comp:
        ligne1 = f"{ligne1}, {comp}"

    adresse_full = " ".join([ligne1, code_postal, commune]).strip()
    return adresse_full, code_postal, commune


def infos_entreprise_depuis_sirene(etab: dict):
    """
    Essaie d'extraire :
      - nom de l'entreprise
      - nom du dirigeant (si personne physique)
    à partir de l'objet 'etablissement' (et uniteLegale) SIRENE 3.11.
    """
    ul = etab.get("uniteLegale", {}) or {}

    nom_entreprise = (
        etab.get("denominationUsuelleEtablissement")
        or ul.get("denominationUsuelle1UniteLegale")
        or ul.get("denominationUniteLegale")
        or ul.get("nomUniteLegale")
    )

    nom_dirigeant = None
    nom = ul.get("nomUniteLegale")
    prenom = ul.get("prenomUsuelUniteLegale") or ul.get("prenom1UniteLegale")
    if nom and prenom:
        nom_dirigeant = f"{prenom} {nom}"

    return nom_entreprise, nom_dirigeant


def geocoder_ban(adresse: str, cp: str, commune: str):
    """Géocode via BAN. Renvoie un Point (WGS84) ou None."""
    params = {"q": adresse, "limit": 1}
    if cp:
        params["postcode"] = cp
    if commune:
        params["city"] = commune

    r = requests.get(BAN_URL, params=params, timeout=10)
    if r.status_code != 200:
        return None

    data = r.json()
    feats = data.get("features", [])
    if not feats:
        return None

    lon, lat = feats[0]["geometry"]["coordinates"]
    return Point(lon, lat)


def commune_est_en_zrr(code_commune: str, communes_zrr: set):
    """
    True  -> commune totalement ou partiellement ZRR
    False -> commune non ZRR
    None  -> code commune inconnu
    """
    if not code_commune:
        return None
    return code_commune in communes_zrr


def siret_qpv_zrr_distance(siret: str) -> dict:
    """
    Retourne un dict avec :
      - siret
      - nom_entreprise
      - nom_dirigeant
      - adresse
      - code_commune
      - in_zrr (True/False/None)
      - zrr_label (nom de la commune ZRR)
      - est_dans_qpv (True/False/None)
      - distance_km (float ou None)
      - a_moins_500m_qpv (bool ou None)
      - qpv_dans_lesquels (liste)
      - qpv_plus_proche (dict)
      - message (erreur éventuelle)
    """
    # Charge les données (avec cache Streamlit)
    qpv_gdf = load_qpv_polygones(QPV_GEO_PATH)
    df_zrr, communes_zrr = load_zrr_data(ZRR_CSV_PATH)

    etab = get_sirene_etab(siret)
    adresse, cp, commune = adresse_depuis_sirene(etab)
    nom_entreprise, nom_dirigeant = infos_entreprise_depuis_sirene(etab)

    # ---- ZRR via code commune SIRENE ----
    code_commune = etab.get("adresseEtablissement", {}).get(
        "codeCommuneEtablissement"
    )
    if code_commune:
        code_commune = str(code_commune).zfill(5)
    in_zrr = commune_est_en_zrr(code_commune, communes_zrr)

    zrr_label = None    # nom de la commune ZRR
    if in_zrr and code_commune:
        row_zrr = df_zrr.loc[df_zrr["CODGEO"] == code_commune]
        if not row_zrr.empty:
            zrr_label = row_zrr.iloc[0].get(ZRR_LIB_COL)

    # ---- Géocodage pour QPV ----
    pt_wgs = geocoder_ban(adresse, cp, commune)
    if pt_wgs is None:
        return {
            "siret": siret,
            "nom_entreprise": nom_entreprise,
            "nom_dirigeant": nom_dirigeant,
            "adresse": adresse,
            "code_commune": code_commune,
            "in_zrr": in_zrr,
            "zrr_label": zrr_label,
            "est_dans_qpv": None,
            "distance_km": None,
            "a_moins_500m_qpv": None,
            "qpv_dans_lesquels": [],
            "qpv_plus_proche": None,
            "message": "Impossible de géocoder l'adresse.",
        }

    # Reprojeter le point en Lambert-93 (EPSG:2154) comme les QPV
    pt_proj = (
        gpd.GeoSeries([pt_wgs], crs="EPSG:4326")
        .to_crs(qpv_gdf.crs)
        .iloc[0]
    )

    # ---- 1. Est-ce que le point est DANS un QPV ? ----
    mask_inside = qpv_gdf.contains(pt_proj)
    qpv_inside = qpv_gdf[mask_inside]
    est_dans_qpv = not qpv_inside.empty

    qpv_dans_lesquels = []
    if est_dans_qpv:
        for _, row in qpv_inside.iterrows():
            qpv_dans_lesquels.append(
                {
                    "code_qp": row.get(COL_CODE_QP),
                    "lib_qp": row.get(COL_LIB_QP),
                    "commune_qp": row.get(COL_LIB_COM),
                }
            )

    # ---- 2. Distance minimale à n'importe quel QPV ----
    distances_m = qpv_gdf.geometry.distance(pt_proj)
    min_dist_m = float(distances_m.min())
    distance_km = min_dist_m / 1000.0
    a_moins_500m_qpv = distance_km <= 0.5

    # QPV le plus proche
    idx_min = distances_m.idxmin()
    row_min = qpv_gdf.loc[idx_min]
    qpv_plus_proche = {
        "code_qp": row_min.get(COL_CODE_QP),
        "lib_qp": row_min.get(COL_LIB_QP),
        "commune_qp": row_min.get(COL_LIB_COM),
        "distance_km": distance_km,
    }

    return {
        "siret": siret,
        "nom_entreprise": nom_entreprise,
        "nom_dirigeant": nom_dirigeant,
        "adresse": adresse,
        "code_commune": code_commune,
        "in_zrr": in_zrr,
        "zrr_label": zrr_label,
        "est_dans_qpv": est_dans_qpv,
        "distance_km": distance_km,
        "a_moins_500m_qpv": a_moins_500m_qpv,
        "qpv_dans_lesquels": qpv_dans_lesquels,
        "qpv_plus_proche": qpv_plus_proche,
        "message": None,
    }


# ================== UI STREAMLIT ==================

st.set_page_config("ZRR & QPV par SIRET", layout="wide")

st.title("🔍 Vérification ZRR & QPV à partir d’un SIRET")

with st.sidebar:
    st.markdown("### ℹ️ À propos")
    st.write(
        "Cet outil interroge l'API SIRENE et l'API Adresse, "
        "puis croise les résultats avec les zonages **ZRR** et **QPV**"
    )
    st.write("1. Saisis un SIRET (avec ou sans espaces)")
    st.write("2. Clique sur **Analyser**")
    st.write("3. Lis les sections **Entreprise**, **ZRR**, **QPV**")

siret_input = st.text_input("SIRET de l'établissement", placeholder="123 456 789 00011")
analyser = st.button("Analyser")

if analyser:
    # nettoyage du SIRET : on garde uniquement les chiffres
    siret_clean = "".join(c for c in siret_input if c.isdigit())

    if len(siret_clean) != 14:
        st.error(
            "Le SIRET doit contenir **14 chiffres** "
            "(tu peux mettre des espaces ou tirets, ils seront ignorés)"
        )
    else:
        with st.spinner("Analyse en cours..."):
            try:
                res = siret_qpv_zrr_distance(siret_clean)
            except Exception as e:
                st.error(f"Erreur lors de l'analyse : {e}")
            else:
                nom_entreprise = res.get("nom_entreprise")
                nom_dirigeant = res.get("nom_dirigeant")
                adresse = res.get("adresse", "(adresse indisponible)")
                code_commune = res.get("code_commune")
                in_zrr = res.get("in_zrr")
                zrr_label = res.get("zrr_label")
                est_dans_qpv = res.get("est_dans_qpv")
                distance_km = res.get("distance_km")
                a_moins_500m = res.get("a_moins_500m_qpv")
                qpv_inside = res.get("qpv_dans_lesquels", [])
                qpv_plus_proche = res.get("qpv_plus_proche")
                msg = res.get("message")

                # ======= PARTIE 1 : ENTREPRISE =======
                st.markdown("## 🏢 Entreprise")
                st.write(f"**Nom :** {nom_entreprise or 'Non disponible'}")
                st.write(f"**SIRET :** {siret_clean}")
                st.write(f"**Adresse :** {adresse}")
                if nom_dirigeant:
                    st.write(f"**Dirigeant :** {nom_dirigeant}")
                if code_commune:
                    st.write(f"**Code commune :** {code_commune}")

                # ======= PARTIE 2 : ZRR =======
                st.markdown("---")
                st.markdown("## 🏔️ ZRR")
                if in_zrr is True:
                    if zrr_label:
                        st.success(
                            "✅ L'entreprise est située dans une **ZRR**.\n\n"
                            f"**Commune ZRR :** {zrr_label}"
                        )
                    else:
                        st.success(
                            "✅ L'entreprise est située dans une **ZRR** "
                            "(nom de la commune non disponible)."
                        )
                elif in_zrr is False:
                    st.error("❌ L'entreprise n'est pas située dans une ZRR.")
                else:
                    st.warning("⚠️ Impossible de déterminer si la commune est en ZRR.")

                # ======= PARTIE 3 : QPV =======
                st.markdown("---")
                st.markdown("## 🏙️ QPV")

                if msg:
                    st.info(msg)

                if distance_km is not None:
                    if a_moins_500m:
                        st.success(
                            f"✅ L'entreprise est à **moins de 500m** d'un QPV "
                        )
                    else:
                        st.info(
                            f"❌ L'entreprise est à **plus de 500m** de tout QPV "
                        )
                else:
                    st.warning(
                        "⚠️ Distance aux QPV non calculée (problème de géocodage)"
                    )

                if qpv_plus_proche is not None:
                    st.write(
                        f"- **QPV le plus proche :** {qpv_plus_proche['lib_qp']} "
                        f"({qpv_plus_proche['commune_qp']})"
                    )
                    st.write(
                        f"- **Distance :** {qpv_plus_proche['distance_km']:.3f} km"
                    )