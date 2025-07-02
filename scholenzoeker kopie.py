import streamlit as st
import pandas as pd
import pathlib
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
DATA_DIR = pathlib.Path("data")

@st.cache_data
def laad_data() -> pd.DataFrame:
    bestanden = list(DATA_DIR.glob("*.csv"))
    alle_kolommen = set()
    verwerkte_bestanden = []

    # Eerste pass: verzamel alle kolommen
    for f in bestanden:
        try:
            df = pd.read_csv(f, dtype=str, sep=';', on_bad_lines='skip')
            alle_kolommen.update(df.columns.tolist())
        except Exception as e:
            logging.error(f"Fout bij kolomscan {f.name}: {e}")

    leeftijd_kolommen = [k for k in alle_kolommen if k.startswith("LEEFTIJD_")]
    leeftijd_kolommen = sorted(leeftijd_kolommen, key=lambda x: int(''.join(filter(str.isdigit, x)) or -1))

    verplichte_kolommen = [
        "GEMEENTENAAM",
        "GEMEENTENAAM_LEERLING",
        "INSTELLINGSNAAM_VESTIGING",
        "SOORT_PO",
        "POSTCODE_LEERLING"
    ]
    verplichte_kolommen = [kol.upper() for kol in verplichte_kolommen]

    alle_data = []
    for f in bestanden:
        try:
            df = pd.read_csv(f, dtype=str, sep=';', on_bad_lines='skip').fillna("0")
            df.columns = df.columns.str.strip().str.upper()

            if "POSTCODE_LEERLING" not in df.columns:
                df["POSTCODE_LEERLING"] = "0000"

            logging.info(f"Ingelezen kolommen {f.name}: {df.columns.tolist()}")

            if not all(kol in df.columns for kol in verplichte_kolommen):
                logging.warning(f"Bestand {f.name} mist verplichte kolommen: wordt overgeslagen")
                continue

            ontbrekend = [kol for kol in leeftijd_kolommen if kol not in df.columns]
            for kol in ontbrekend:
                df[kol] = "0"

            for kol in leeftijd_kolommen:
                df[kol] = df[kol].replace("<5", "4").astype(int)

            df["bronbestand"] = f.name

            df_melt = df.melt(
                id_vars=verplichte_kolommen + ["bronbestand"],
                value_vars=leeftijd_kolommen,
                var_name="leeftijd_label",
                value_name="aantal"
            )
            df_melt["leeftijd"] = df_melt["leeftijd_label"].str.extract(r'(\d+)').astype(float)
            alle_data.append(df_melt)
            verwerkte_bestanden.append(f.name)
        except Exception as e:
            logging.error(f"Fout bij inlezen {f.name}: {e}")

    if not alle_data:
        return pd.DataFrame(columns=verplichte_kolommen + ["leeftijd_label", "aantal", "leeftijd", "bronbestand"])

    df_concat = pd.concat(alle_data, ignore_index=True)
    df_concat.attrs['processed_files'] = verwerkte_bestanden
    return df_concat

def main():
    st.title("Leerlingenvervoer Analyse")

    df = laad_data()

    # 🧭 Selecties bovenaan de sidebar
    gemeente_opties = sorted(df["GEMEENTENAAM"].dropna().unique())
    gemeente = st.sidebar.selectbox("📍 Gemeente", gemeente_opties)

    df_gemeente = df[df["GEMEENTENAAM"] == gemeente]

    soorten = sorted(df_gemeente["SOORT_PO"].dropna().unique())
    geselecteerde_soorten = st.sidebar.multiselect("🏫 Soort onderwijs", soorten, default=soorten)

    df_gemeente = df_gemeente[df_gemeente["SOORT_PO"].isin(geselecteerde_soorten)]

    scholen = ["Alle"] + sorted(df_gemeente["INSTELLINGSNAAM_VESTIGING"].dropna().unique())
    school = st.sidebar.selectbox("🏢 School (optioneel)", scholen)

    leeftijd_min, leeftijd_max = st.sidebar.slider("📊 Leeftijd filter", 4, 24, (4, 12))

    # 📄 Verwerkte bestanden onderaan
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Verwerkte bestanden")
    verwerkte_bestanden = df.attrs.get("processed_files", [])
    if verwerkte_bestanden:
        for bestand in verwerkte_bestanden:
            st.sidebar.write(f"• {bestand}")
    else:
        st.sidebar.warning("⚠️ Geen bestanden succesvol verwerkt.")
    df_filtered = df_gemeente[(df_gemeente["leeftijd"] >= leeftijd_min) & (df_gemeente["leeftijd"] <= leeftijd_max)]

    if school != "Alle":
        df_filtered = df_filtered[df_filtered["INSTELLINGSNAAM_VESTIGING"] == school]

    #st.write("Aantal rijen na filtering:", len(df_filtered))
    #st.dataframe(df_filtered.head(20))

    resultaat = df_filtered.groupby("GEMEENTENAAM_LEERLING")["aantal"].sum().reset_index()
    resultaat = resultaat.sort_values("aantal", ascending=False)

    col1, col2 = st.columns([1, 2])

    with col1:

        st.subheader("Leerlingstromen naar gekozen bestemming")
        st.dataframe(resultaat, use_container_width=True)
    with col2:
     
        st.subheader("Kaart van herkomstpostcodes")
    # Laad coördinaten en teken kaart
        try:
            postcode_coords = pd.read_csv("data/postcode_coords.csv", sep=";", dtype=str)
            postcode_coords.columns = postcode_coords.columns.str.strip().str.upper()
            postcode_coords["LAT"] = postcode_coords["LAT"].str.replace(",", ".").astype(float)
            postcode_coords["LON"] = postcode_coords["LON"].str.replace(",", ".").astype(float)
            postcode_coords["POSTCODE"] = postcode_coords["POSTCODE"].str[:4]

            df_filtered["PC4"] = df_filtered["POSTCODE_LEERLING"].str[:4]
            agg = df_filtered.groupby("PC4")["aantal"].sum().reset_index()
            kaartdata = agg.merge(postcode_coords, left_on="PC4", right_on="POSTCODE")

            #st.map(kaartdata[["LAT", "LON"]].assign(size=kaartdata["aantal"]))

            with st.container():
                st.markdown(
                    "<div style='width: 60%; max-width: 800px;'>",
                    unsafe_allow_html=True
                )
                st.map(kaartdata[["LAT", "LON"]].assign(size=kaartdata["aantal"]))
                st.markdown("</div>", unsafe_allow_html=True)

            
        except Exception as e:
            st.warning(f"Kon geen kaart maken: {e}")

if __name__ == "__main__":
    main()