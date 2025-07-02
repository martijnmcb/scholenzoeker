from nicegui import ui, events
import pandas as pd
import pathlib
import logging
import numpy as np
import folium
import tempfile

logging.basicConfig(level=logging.INFO)
DATA_DIR = pathlib.Path("data")
DATA_DIR2 = pathlib.Path(__file__).parent / "data"

def genereer_kaart(kaartdata: pd.DataFrame) -> None:
    m = folium.Map(location=[kaartdata["LAT"].mean(), kaartdata["LON"].mean()], zoom_start=10)
    for _, row in kaartdata.iterrows():
        folium.CircleMarker(
            location=[row["LAT"], row["LON"]],
            radius=6,
            popup=f"{row['INSTELLINGSNAAM_VESTIGING']} ({int(row['aantal_leerlingen'])})",
            color="blue",
            fill=True,
            fill_opacity=0.6,
        ).add_to(m)
    kaart_pad = DATA_DIR / "kaart.html"
    m.save(kaart_pad)

def laad_data() -> pd.DataFrame:
    bestanden = list(DATA_DIR.glob("*.csv"))
    print(f"Gevonden bestanden in {DATA_DIR.resolve()}: {bestanden}")
    alle_kolommen = set()
    verwerkte_bestanden = []

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

@ui.page('/')
def leerlingenvervoer_analyse():
    df = laad_data()
    kolommen = ["GEMEENTENAAM", "GEMEENTENAAM_LEERLING", "INSTELLINGSNAAM_VESTIGING", "SOORT_PO", "POSTCODE_LEERLING"]

    if not df.empty:
        df = df[kolommen]
        gemeenten = sorted(df["GEMEENTENAAM"].dropna().unique())
        with ui.row().classes('w-full items-center gap-4'):
            with ui.column():
                ui.label('Selecteer gemeente')
                gemeente_select = ui.select(gemeenten).on('update:model-value', lambda e: (update_scholenlijst(), update_tabel()))
            with ui.column():
                ui.label('Selecteer school')
                school_select = ui.select([]).on('update:model-value', lambda e: update_tabel())

        def update_scholenlijst():
            if not gemeente_select.value:
                school_select.options = []
                return
            scholen = sorted(df[df["GEMEENTENAAM"] == gemeente_select.value]["INSTELLINGSNAAM_VESTIGING"].dropna().unique())
            school_select.options = ["Alle scholen"] + scholen
            if scholen:
                school_select.value = scholen[0]

        with ui.row().classes('w-full items-start no-wrap'):
            kaart_output = ui.column().classes('w-1/2')
            output = ui.column().classes('w-1/2')

        def update_tabel():
            if not gemeente_select.value:
                return  # geen gemeente gekozen

            kaart_output.clear()

            filtered = df[df["GEMEENTENAAM"] == gemeente_select.value]

            if school_select.value != "Alle scholen" and school_select.value in filtered["INSTELLINGSNAAM_VESTIGING"].dropna().unique():
                filtered = filtered[filtered["INSTELLINGSNAAM_VESTIGING"] == school_select.value]

            if "aantal" in filtered.columns:
                resultaat = filtered.groupby(["INSTELLINGSNAAM_VESTIGING", "GEMEENTENAAM_LEERLING"])["aantal"].sum().reset_index(name="aantal_leerlingen")
            else:
                resultaat = filtered.groupby(["INSTELLINGSNAAM_VESTIGING", "GEMEENTENAAM_LEERLING"]).size().reset_index(name="aantal_leerlingen")

            resultaat = resultaat.head(100)
            try:
                postcode_coords = pd.read_csv(DATA_DIR / "postcode_coords.csv", sep=";", dtype=str)
                postcode_coords.columns = postcode_coords.columns.str.strip().str.upper()
                postcode_coords["LAT"] = postcode_coords["LAT"].str.replace(",", ".").astype(float)
                postcode_coords["LON"] = postcode_coords["LON"].str.replace(",", ".").astype(float)
                postcode_coords["POSTCODE"] = postcode_coords["POSTCODE"].str[:4]

                merged = resultaat.merge(filtered[["INSTELLINGSNAAM_VESTIGING", "GEMEENTENAAM_LEERLING", "POSTCODE_LEERLING"]].drop_duplicates(),
                                        on=["INSTELLINGSNAAM_VESTIGING", "GEMEENTENAAM_LEERLING"], how="left")
                merged["PC4"] = merged["POSTCODE_LEERLING"].str[:4]

                kaartdata = merged.merge(postcode_coords, left_on="PC4", right_on="POSTCODE")
                print("Kaartdata preview:\n", kaartdata.head())

                genereer_kaart(kaartdata)

                with kaart_output:
                    ui.label("Kaart herkomst leerlingen").classes("text-h4")
                    ui.html(f'<iframe src="/data/kaart.html?reload={np.random.randint(0, 1e6)}" style="width: 650px; height: 600px;" class="w-full"></iframe>')
            except Exception as e:
                with kaart_output:
                    ui.label(f"Kon geen kaart tonen: {e}").classes("text-red")

            output.clear()
            with output:
                ui.label("Tabel leerlinggegevens").classes("text-h4")
                ui.table(
                    columns=[{"name": c, "label": c, "field": c} for c in resultaat.columns],
                    rows=resultaat.to_dict("records")
                ).classes("w-full")

        
        pass  # initieel niets tonen tot gemeente gekozen is
    else:
        ui.label("Geen data gevonden in de map /data").classes("text-h6 text-red")

from nicegui import app
app.add_static_files('/data', DATA_DIR)
ui.run()    