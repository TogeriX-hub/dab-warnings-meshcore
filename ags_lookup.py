"""
ags_lookup.py – WarnBridge
Lädt die offizielle Gemeindeschlüssel-Liste (Destatis) einmalig beim Start,
cached sie lokal als ags_cache.json.

Funktionen:
- find_district(name) → AGS-Kreis (5-stellig) oder None
- get_all_states() → dict {state_code: state_name}
- get_districts_for_state(state_code) → list [{ags, name}]
- get_municipalities_for_district(district_ags) → list [{ags, name}]
"""

import json
import logging
import re
import urllib.request
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Offizielle Destatis Gemeindeschlüssel-Liste
# Wird einmalig heruntergeladen und lokal gecacht
DESTATIS_URL = (
    "https://www.xrepository.de/api/xrepository/"
    "urn:de:bund:destatis:bevoelkerungsstatistik:schluessel:rs_2021-07-31"
    "/download/Regionalschl_ssel_2021-07-31.json"
)

CACHE_PATH = Path("ags_cache.json")

# Interne Struktur nach dem Laden:
# _municipalities: dict {normalized_name: [{"ags12": "...", "name": "...", "district_ags": "...", "state_ags": "..."}]}
# _districts: dict {ags5: {"name": "...", "state_ags": "..."}}
# _states: dict {ags2: name}

_municipalities: dict[str, list[dict]] = {}
_districts: dict[str, dict] = {}
_states: dict[str, str] = {}
_loaded = False


def _normalize(name: str) -> str:
    """Name normalisieren für Suche: lowercase, Umlaute ersetzen, Sonderzeichen weg."""
    n = name.lower().strip()
    n = n.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    n = re.sub(r"[^a-z0-9\s\-]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _ags12_to_district(ags12: str) -> str:
    """12-stelligen AGS auf 5-stelligen Kreis-AGS kürzen."""
    return ags12[:5]


def _ags12_to_state(ags12: str) -> str:
    """12-stelligen AGS auf 2-stelligen Bundesland-AGS kürzen."""
    return ags12[:2]


# Alle 16 Bundesländer
_BUNDESLAND_NAMES = {
    "01": "Schleswig-Holstein", "02": "Hamburg", "03": "Niedersachsen",
    "04": "Bremen", "05": "Nordrhein-Westfalen", "06": "Hessen",
    "07": "Rheinland-Pfalz", "08": "Baden-Württemberg", "09": "Bayern",
    "10": "Saarland", "11": "Berlin", "12": "Brandenburg",
    "13": "Mecklenburg-Vorpommern", "14": "Sachsen", "15": "Sachsen-Anhalt",
    "16": "Thüringen",
}

# Alle deutschen Landkreise (AGS5 → Name)
# Kreisfreie Städte kommen aus der Destatis-Liste, Landkreise sind hier eingebettet
_LANDKREIS_NAMES = {
    # Baden-Württemberg
    "08115": "Landkreis Böblingen", "08116": "Landkreis Esslingen",
    "08117": "Landkreis Göppingen", "08118": "Landkreis Ludwigsburg",
    "08119": "Rems-Murr-Kreis", "08125": "Landkreis Heilbronn",
    "08126": "Hohenlohekreis", "08127": "Landkreis Schwäbisch Hall",
    "08128": "Main-Tauber-Kreis", "08135": "Landkreis Heidenheim",
    "08136": "Ostalbkreis", "08215": "Landkreis Karlsruhe",
    "08216": "Landkreis Rastatt", "08225": "Neckar-Odenwald-Kreis",
    "08226": "Rhein-Neckar-Kreis", "08235": "Landkreis Calw",
    "08236": "Enzkreis", "08237": "Landkreis Freudenstadt",
    "08315": "Landkreis Breisgau-Hochschwarzwald", "08316": "Landkreis Emmendingen",
    "08317": "Ortenaukreis", "08325": "Landkreis Rottweil",
    "08326": "Schwarzwald-Baar-Kreis", "08327": "Landkreis Tuttlingen",
    "08335": "Landkreis Konstanz", "08336": "Landkreis Lörrach",
    "08337": "Landkreis Waldshut", "08415": "Landkreis Reutlingen",
    "08416": "Landkreis Tübingen", "08417": "Zollernalbkreis",
    "08425": "Alb-Donau-Kreis", "08426": "Landkreis Biberach",
    "08435": "Bodenseekreis", "08436": "Landkreis Ravensburg",
    "08437": "Landkreis Sigmaringen",
    # Bayern
    "09171": "Landkreis Aichach-Friedberg", "09172": "Landkreis Augsburg",
    "09173": "Landkreis Dillingen a.d.Donau", "09174": "Landkreis Günzburg",
    "09175": "Landkreis Neu-Ulm", "09176": "Landkreis Lindau (Bodensee)",
    "09177": "Landkreis Ostallgäu", "09178": "Landkreis Unterallgäu",
    "09179": "Landkreis Donau-Ries", "09180": "Landkreis Oberallgäu",
    "09181": "Landkreis Altötting", "09182": "Landkreis Berchtesgadener Land",
    "09183": "Landkreis Bad Tölz-Wolfratshausen", "09184": "Landkreis Dachau",
    "09185": "Landkreis Ebersberg", "09186": "Landkreis Eichstätt",
    "09187": "Landkreis Erding", "09188": "Landkreis Freising",
    "09189": "Landkreis Fürstenfeldbruck", "09190": "Landkreis Garmisch-Partenkirchen",
    "09191": "Landkreis Landsberg am Lech", "09192": "Landkreis Miesbach",
    "09193": "Landkreis Mühldorf a.Inn", "09194": "Landkreis München",
    "09195": "Landkreis Neuburg-Schrobenhausen", "09196": "Landkreis Pfaffenhofen a.d.Ilm",
    "09197": "Landkreis Rosenheim", "09198": "Landkreis Starnberg",
    "09199": "Landkreis Traunstein", "09200": "Landkreis Weilheim-Schongau",
    "09271": "Landkreis Amberg-Sulzbach", "09272": "Landkreis Cham",
    "09273": "Landkreis Neumarkt i.d.OPf.", "09274": "Landkreis Neustadt a.d.Waldnaab",
    "09275": "Landkreis Regensburg", "09276": "Landkreis Schwandorf",
    "09277": "Landkreis Tirschenreuth",
    "09371": "Landkreis Bamberg", "09372": "Landkreis Bayreuth",
    "09373": "Landkreis Coburg", "09374": "Landkreis Forchheim",
    "09375": "Landkreis Hof", "09376": "Landkreis Kronach",
    "09377": "Landkreis Kulmbach", "09378": "Landkreis Lichtenfels",
    "09379": "Landkreis Wunsiedel i.Fichtelgebirge",
    "09278": "Landkreis Neustadt a.d.Waldnaab", "09279": "Landkreis Tirschenreuth",
    "09471": "Landkreis Ansbach", "09472": "Landkreis Erlangen-Höchstadt",
    "09473": "Landkreis Fürth", "09474": "Landkreis Nürnberger Land",
    "09475": "Landkreis Neustadt a.d.Aisch-Bad Windsheim",
    "09476": "Landkreis Roth", "09477": "Landkreis Weißenburg-Gunzenhausen",
    "09478": "Landkreis Ansbach (Ergänzung)", "09479": "Landkreis Neustadt a.d.Aisch",
    "09571": "Landkreis Aschaffenburg", "09572": "Landkreis Bad Kissingen",
    "09573": "Landkreis Rhön-Grabfeld", "09574": "Landkreis Haßberge",
    "09575": "Landkreis Kitzingen", "09576": "Landkreis Miltenberg",
    "09577": "Landkreis Main-Spessart", "09578": "Landkreis Schweinfurt",
    "09579": "Landkreis Würzburg",
    "09671": "Landkreis Deggendorf", "09672": "Landkreis Freyung-Grafenau",
    "09673": "Landkreis Kelheim", "09674": "Landkreis Landshut",
    "09675": "Landkreis Passau", "09676": "Landkreis Regen",
    "09677": "Landkreis Rottal-Inn", "09678": "Landkreis Straubing-Bogen",
    "09679": "Landkreis Dingolfing-Landau",
    "09771": "Landkreis Garmisch-Partenkirchen", "09772": "Landkreis Landsberg am Lech",
    "09773": "Landkreis Miesbach", "09774": "Landkreis Mühldorf a.Inn",
    "09775": "Landkreis München", "09776": "Landkreis Neuburg-Schrobenhausen",
    "09777": "Landkreis Pfaffenhofen a.d.Ilm", "09778": "Landkreis Rosenheim",
    "09779": "Landkreis Starnberg", "09780": "Landkreis Traunstein",
    # Niedersachsen
    "03151": "Landkreis Gifhorn", "03152": "Landkreis Göttingen",
    "03153": "Landkreis Goslar", "03154": "Landkreis Helmstedt",
    "03155": "Landkreis Northeim", "03156": "Landkreis Osterode am Harz",
    "03157": "Landkreis Peine", "03158": "Landkreis Wolfenbüttel",
    "03241": "Landkreis Diepholz", "03251": "Landkreis Hameln-Pyrmont",
    "03252": "Landkreis Hannover", "03254": "Landkreis Holzminden",
    "03255": "Landkreis Nienburg/Weser", "03256": "Landkreis Schaumburg",
    "03351": "Landkreis Celle", "03352": "Landkreis Cuxhaven",
    "03353": "Landkreis Harburg", "03354": "Landkreis Lüchow-Dannenberg",
    "03355": "Landkreis Lüneburg", "03356": "Landkreis Osterholz",
    "03357": "Landkreis Rotenburg (Wümme)", "03358": "Landkreis Heidekreis",
    "03359": "Landkreis Stade", "03360": "Landkreis Uelzen",
    "03361": "Landkreis Verden",
    "03401": "Landkreis Ammerland", "03402": "Landkreis Aurich",
    "03403": "Landkreis Cloppenburg", "03404": "Landkreis Emsland",
    "03405": "Landkreis Friesland", "03406": "Landkreis Grafschaft Bentheim",
    "03407": "Landkreis Leer", "03408": "Landkreis Oldenburg",
    "03409": "Landkreis Osnabrück", "03410": "Landkreis Vechta",
    "03451": "Landkreis Wesermarsch", "03452": "Landkreis Wittmund",
    # NRW
    "05154": "Landkreis Kleve", "05158": "Rhein-Kreis Neuss",
    "05162": "Landkreis Viersen", "05166": "Landkreis Wesel",
    "05314": "Landkreis Aachen", "05316": "Landkreis Düren",
    "05334": "Rhein-Erft-Kreis", "05358": "Landkreis Düren",
    "05362": "Rhein-Erft-Kreis", "05366": "Landkreis Euskirchen",
    "05370": "Landkreis Heinsberg", "05374": "Rhein-Kreis Neuss",
    "05378": "Rheinisch-Bergischer Kreis", "05382": "Rhein-Sieg-Kreis",
    "05512": "Landkreis Borken", "05515": "Landkreis Coesfeld",
    "05554": "Landkreis Recklinghausen", "05558": "Landkreis Steinfurt",
    "05562": "Landkreis Warendorf",
    "05711": "Landkreis Gütersloh", "05754": "Landkreis Herford",
    "05758": "Landkreis Höxter", "05762": "Landkreis Lippe",
    "05766": "Landkreis Minden-Lübbecke", "05770": "Landkreis Paderborn",
    "05911": "Landkreis Ennepe-Ruhr-Kreis", "05958": "Hochsauerlandkreis",
    "05962": "Landkreis Märkisches Sauerland", "05966": "Landkreis Olpe",
    "05970": "Landkreis Siegen-Wittgenstein", "05974": "Landkreis Soest",
    "05978": "Landkreis Unna",
    # Hessen
    "06431": "Landkreis Bergstraße", "06432": "Landkreis Darmstadt-Dieburg",
    "06433": "Landkreis Groß-Gerau", "06434": "Landkreis Hochtaunuskreis",
    "06435": "Main-Kinzig-Kreis", "06436": "Main-Taunus-Kreis",
    "06437": "Landkreis Odenwaldkreis", "06438": "Landkreis Offenbach",
    "06439": "Landkreis Rheingau-Taunus-Kreis", "06440": "Landkreis Wetteraukreis",
    "06531": "Landkreis Gießen", "06532": "Landkreis Lahn-Dill-Kreis",
    "06533": "Landkreis Limburg-Weilburg", "06534": "Landkreis Marburg-Biedenkopf",
    "06535": "Landkreis Vogelsbergkreis",
    "06611": "Landkreis Fulda", "06631": "Landkreis Hersfeld-Rotenburg",
    "06632": "Landkreis Kassel", "06633": "Landkreis Schwalm-Eder-Kreis",
    "06634": "Landkreis Waldeck-Frankenberg", "06635": "Landkreis Werra-Meißner-Kreis",
    # Rheinland-Pfalz
    "07131": "Landkreis Ahrweiler", "07132": "Landkreis Altenkirchen (Westerwald)",
    "07133": "Bad Kreuznach", "07134": "Landkreis Birkenfeld",
    "07135": "Landkreis Cochem-Zell", "07137": "Landkreis Mayen-Koblenz",
    "07138": "Landkreis Neuwied", "07140": "Landkreis Rhein-Hunsrück-Kreis",
    "07141": "Landkreis Rhein-Lahn-Kreis", "07143": "Landkreis Westerwaldkreis",
    "07231": "Landkreis Bernkastel-Wittlich", "07232": "Landkreis Bitburg-Prüm",
    "07233": "Landkreis Vulkaneifel", "07235": "Landkreis Trier-Saarburg",
    "07331": "Landkreis Alzey-Worms", "07332": "Landkreis Bad Dürkheim",
    "07333": "Landkreis Donnersbergkreis", "07334": "Landkreis Germersheim",
    "07335": "Landkreis Kaiserslautern", "07336": "Landkreis Kusel",
    "07337": "Landkreis Südliche Weinstraße", "07338": "Landkreis Rhein-Pfalz-Kreis",
    "07339": "Landkreis Mainz-Bingen", "07340": "Landkreis Südwestpfalz",
    # Schleswig-Holstein
    "01051": "Landkreis Dithmarschen", "01053": "Landkreis Herzogtum Lauenburg",
    "01054": "Landkreis Nordfriesland", "01055": "Landkreis Ostholstein",
    "01056": "Landkreis Pinneberg", "01057": "Landkreis Plön",
    "01058": "Landkreis Rendsburg-Eckernförde", "01059": "Landkreis Schleswig-Flensburg",
    "01060": "Landkreis Segeberg", "01061": "Landkreis Steinburg",
    "01062": "Landkreis Stormarn",
    # Brandenburg
    "12051": "Landkreis Barnim", "12052": "Landkreis Dahme-Spreewald",
    "12053": "Landkreis Elbe-Elster", "12054": "Landkreis Havelland",
    "12060": "Landkreis Märkisch-Oderland", "12061": "Landkreis Oberhavel",
    "12062": "Landkreis Oberspreewald-Lausitz", "12063": "Landkreis Oder-Spree",
    "12064": "Landkreis Ostprignitz-Ruppin", "12065": "Landkreis Potsdam-Mittelmark",
    "12066": "Landkreis Prignitz", "12067": "Landkreis Spree-Neiße",
    "12068": "Landkreis Teltow-Fläming", "12069": "Landkreis Uckermark",
    # Sachsen
    "14511": "Erzgebirgskreis", "14521": "Landkreis Mittelsachsen",
    "14522": "Landkreis Nordsachsen", "14523": "Landkreis Leipzig",
    "14524": "Landkreis Sächsische Schweiz-Osterzgebirge",
    "14525": "Landkreis Bautzen", "14626": "Landkreis Görlitz",
    "14627": "Landkreis Meißen", "14628": "Landkreis Zwickau",
    # Sachsen-Anhalt
    "15081": "Landkreis Altmarkkreis Salzwedel", "15082": "Landkreis Anhalt-Bitterfeld",
    "15083": "Landkreis Börde", "15084": "Landkreis Burgenlandkreis",
    "15085": "Landkreis Harz", "15086": "Landkreis Jerichower Land",
    "15087": "Landkreis Mansfeld-Südharz", "15088": "Landkreis Saalekreis",
    "15089": "Landkreis Salzlandkreis", "15090": "Landkreis Stendal",
    "15091": "Landkreis Wittenberg",
    # Thüringen
    "16061": "Landkreis Eichsfeld", "16062": "Landkreis Nordhausen",
    "16063": "Landkreis Wartburgkreis", "16064": "Landkreis Unstrut-Hainich-Kreis",
    "16065": "Landkreis Kyffhäuserkreis", "16066": "Landkreis Schmalkalden-Meiningen",
    "16067": "Landkreis Gotha", "16068": "Landkreis Sömmerda",
    "16069": "Landkreis Hildburghausen", "16070": "Landkreis Ilm-Kreis",
    "16071": "Landkreis Weimarer Land", "16072": "Landkreis Sonneberg",
    "16073": "Landkreis Saalfeld-Rudolstadt", "16074": "Landkreis Saale-Holzland-Kreis",
    "16075": "Landkreis Saale-Orla-Kreis", "16076": "Landkreis Greiz",
    "16077": "Landkreis Altenburger Land",
    # Mecklenburg-Vorpommern
    "13071": "Landkreis Rostock", "13072": "Landkreis Schwerin",
    "13073": "Landkreis Mecklenburgische Seenplatte",
    "13074": "Landkreis Vorpommern-Rügen", "13075": "Landkreis Nordwestmecklenburg",
    "13076": "Landkreis Vorpommern-Greifswald",
    "13077": "Landkreis Ludwigslust-Parchim",
    # Saarland
    "10041": "Landkreis Merzig-Wadern", "10042": "Landkreis Neunkirchen",
    "10043": "Landkreis St. Wendel", "10044": "Saarpfalz-Kreis",
    "10045": "Landkreis Saarlouis", "10046": "Landkreis St. Wendel",
    # Niedersachsen (fehlende)
    "03159": "Landkreis Göttingen", "03257": "Landkreis Schaumburg",
    "03453": "Landkreis Cloppenburg", "03454": "Landkreis Emsland",
    "03455": "Landkreis Friesland", "03456": "Landkreis Grafschaft Bentheim",
    "03457": "Landkreis Leer", "03458": "Landkreis Oldenburg",
    "03459": "Landkreis Osnabrück", "03460": "Landkreis Vechta",
    "03461": "Landkreis Wesermarsch", "03462": "Landkreis Wittmund",
    "03901": "Küstengewässer Nordsee",
    # NRW (fehlende)
    "05170": "Landkreis Wesel", "05566": "Landkreis Steinfurt",
    "05570": "Landkreis Warendorf", "05774": "Landkreis Paderborn",
    "05954": "Ennepe-Ruhr-Kreis",
    # Hessen (fehlende)
    "06636": "Landkreis Werra-Meißner-Kreis",
    # Rheinland-Pfalz (fehlende)
    "07000": "Rheinland-Pfalz (überregional)",
    # Berlin Bezirke
    "11001": "Berlin Mitte", "11002": "Berlin Friedrichshain-Kreuzberg",
    "11003": "Berlin Pankow", "11004": "Berlin Charlottenburg-Wilmersdorf",
    "11005": "Berlin Spandau", "11006": "Berlin Steglitz-Zehlendorf",
    "11007": "Berlin Tempelhof-Schöneberg", "11008": "Berlin Neukölln",
    "11009": "Berlin Treptow-Köpenick", "11010": "Berlin Marzahn-Hellersdorf",
    "11011": "Berlin Lichtenberg", "11012": "Berlin Reinickendorf",
    # Hamburg Bezirke
    "02101": "Hamburg-Mitte", "02102": "Hamburg Altona",
    "02103": "Hamburg Eimsbüttel", "02104": "Hamburg Nord",
    "02105": "Hamburg Wandsbek", "02106": "Hamburg Bergedorf",
    "02107": "Hamburg Harburg",
    # Brandenburg (fehlende)
    "12070": "Landkreis Prignitz", "12071": "Landkreis Spree-Neiße",
    "12072": "Landkreis Teltow-Fläming", "12073": "Landkreis Uckermark",
    # Mecklenburg-Vorpommern (fehlende)
    "13000": "Mecklenburg-Vorpommern Küstengewässer",
    # Sachsen (fehlende)
    "14625": "Landkreis Bautzen", "14729": "Landkreis Leipzig",
    "14730": "Landkreis Nordsachsen",
    # Bayern (fehlende)
    "09278": "Landkreis Neustadt a.d.Waldnaab",
    "09279": "Landkreis Tirschenreuth",
    "09478": "Landkreis Ansbach (Nord)",
    "09479": "Landkreis Neustadt a.d.Aisch-Bad Windsheim",
}


def _build_index(raw: list):
    """
    Rohdaten aus Destatis-JSON in interne Strukturen umwandeln.
    Format: [[ags12, name, None], ...]

    Die Destatis-Liste enthält nur kreisfreie Städte, keine Landkreise.
    Landkreise werden aus AGS5-Präfix der Gemeinden abgeleitet.
    Namen aus _LANDKREIS_NAMES oder Fallback "Kreis XXXXX".
    """
    global _municipalities, _districts, _states

    _municipalities = {}
    _districts = {}
    _states = dict(_BUNDESLAND_NAMES)  # Alle 16 Bundesländer vorbelegen

    for entry in raw:
        if not isinstance(entry, (list, tuple)) or len(entry) < 2:
            continue
        ags12 = str(entry[0]).strip()
        name = str(entry[1]).strip()
        if not ags12 or not name:
            continue

        ags_stripped = ags12.rstrip("0")
        ags_len = len(ags_stripped)

        if ags_len <= 2:
            # Bundesland – überschreibe Vorbelgung mit echtem Namen
            _states[ags12[:2]] = name

        elif ags_len <= 5:
            # Kreisfreie Stadt
            district_ags = ags12[:5]
            _districts[district_ags] = {"name": name, "state_ags": ags12[:2]}

        else:
            # Gemeinde → Kreis aus ersten 5 Stellen ableiten
            state_ags = ags12[:2]

            # Stadtstaaten (Hamburg 02, Bremen 04, Berlin 11) haben keine echten
            # Landkreise. Alle Untereinheiten werden einem einzigen Eintrag zugeordnet
            # damit nicht ~100 "Kreis 021xx"-Phantomkreise entstehen.
            _STADTSTAATEN = {"02": "02000", "04": "04000", "11": "11000"}
            _STADTSTAAT_NAMEN = {"02000": "Hamburg", "04000": "Bremen", "11000": "Berlin"}
            if state_ags in _STADTSTAATEN:
                district_ags = _STADTSTAATEN[state_ags]
                if district_ags not in _districts:
                    _districts[district_ags] = {
                        "name": _STADTSTAAT_NAMEN[district_ags],
                        "state_ags": state_ags,
                    }
            else:
                district_ags = ags12[:5]
                if district_ags not in _districts:
                    kreis_name = _LANDKREIS_NAMES.get(district_ags, f"Kreis {district_ags}")
                    _districts[district_ags] = {"name": kreis_name, "state_ags": state_ags}

            norm = _normalize(name)
            record = {
                "ags12": ags12,
                "name": name,
                "district_ags": district_ags,
                "state_ags": state_ags,
            }
            if norm not in _municipalities:
                _municipalities[norm] = []
            _municipalities[norm].append(record)

    logger.info(
        "AGS-Lookup geladen: %d Bundesländer, %d Kreise, %d Gemeinden",
        len(_states), len(_districts), len(_municipalities)
    )


def _load_from_cache() -> bool:
    """Cache laden falls vorhanden."""
    if not CACHE_PATH.exists():
        return False
    try:
        data = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        _build_index(data)
        logger.info("AGS-Cache geladen: %s", CACHE_PATH)
        return True
    except Exception as e:
        logger.warning("AGS-Cache ungültig, lade neu: %s", e)
        return False


def _download_and_cache() -> bool:
    """Offizielle Liste herunterladen und cachen."""
    logger.info("Lade Gemeindeschlüssel von Destatis...")
    try:
        req = urllib.request.Request(DESTATIS_URL, headers={"User-Agent": "WarnBridge/1.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = json.loads(resp.read().decode("utf-8"))

        # Destatis-Format: direkt eine Liste [[ags12, name, ebene], ...]
        # oder Dict mit "daten"-Key
        if isinstance(raw, dict):
            data = raw.get("daten", raw.get("data", []))
        else:
            data = raw  # direkt die Liste

        CACHE_PATH.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        logger.info("AGS-Cache gespeichert: %s (%d Einträge)", CACHE_PATH, len(data))
        _build_index(data)
        return True

    except Exception as e:
        logger.error("Fehler beim Laden der Gemeindeschlüssel: %s", e)
        return False


def load(force_download: bool = False) -> bool:
    """
    Beim Start aufrufen. Lädt Cache oder lädt von Destatis herunter.
    Gibt True zurück wenn erfolgreich.
    """
    global _loaded
    if _loaded and not force_download:
        return True

    if not force_download and _load_from_cache():
        _loaded = True
        return True

    if _download_and_cache():
        _loaded = True
        return True

    logger.warning("AGS-Lookup nicht verfügbar – /warnings [Ort] eingeschränkt")
    return False


# ---------------------------------------------------------------------------
# Öffentliche API
# ---------------------------------------------------------------------------

def find_district(name: str) -> Optional[str]:
    """
    Gemeinde- oder Kreisname → AGS-Kreis (5-stellig).
    Gibt den besten Treffer zurück.
    Beispiel: "Sindelfingen" → "08115", "Karlsruhe" → "08212"
    """
    if not _loaded:
        return None

    # Bekannte Aliase für mehrdeutige Städtenamen
    _ALIASES = {
        "frankfurt": "06412",       # Frankfurt am Main
        "frankfurt am main": "06412",
        "koeln": "05315",
        "köln": "05315",
        "muenchen": "09162",        # München Stadt
        "münchen": "09162",
        "nuernberg": "09564",
        "nürnberg": "09564",
        "hannover": "03241",
        "bremen": "04011",
        "dresden": "14612",
        "leipzig": "14713",
        "duesseldorf": "05111",
        "düsseldorf": "05111",
        "leonberg": "08115",        # Leonberg im Landkreis Böblingen BW
    }

    norm = _normalize(name)

    # 0. Alias-Check
    if norm in _ALIASES:
        return _ALIASES[norm]

    # 1. Exakte Gemeinde-Suche (normalisierter Name == Suchbegriff)
    if norm in _municipalities:
        records = sorted(_municipalities[norm], key=lambda r: r["ags12"])
        return records[0]["district_ags"]

    # 2. Exakte Kreis-Suche
    for ags5, info in _districts.items():
        if _normalize(info["name"]) == norm:
            return ags5

    # 3. Kreis-Teilstring
    kreis_matches = [
        (ags5, info) for ags5, info in _districts.items()
        if norm in _normalize(info["name"])
    ]
    if kreis_matches:
        kreis_matches.sort(key=lambda x: len(x[1]["name"]))
        return kreis_matches[0][0]

    # 4. Gemeinde-Teilstring: exaktes Wort bevorzugen
    exact_word_matches = []
    substring_matches = []

    for key, records in _municipalities.items():
        key_words = key.split()
        if norm in key_words:
            exact_word_matches.append(records[0])
        elif norm in key or key in norm:
            substring_matches.append(records[0])

    if exact_word_matches:
        exact_word_matches.sort(key=lambda r: len(r["name"]))
        return exact_word_matches[0]["district_ags"]

    if substring_matches:
        substring_matches.sort(key=lambda r: len(r["name"]))
        return substring_matches[0]["district_ags"]

    return None


def find_districts(name: str) -> list[str]:
    """
    Alle passenden Kreise für einen Namen.
    Nützlich wenn ein Ortsname in mehreren Kreisen vorkommt.
    """
    if not _loaded:
        return []

    norm = _normalize(name)
    found: set[str] = set()

    # Exakt
    if norm in _municipalities:
        for r in _municipalities[norm]:
            found.add(r["district_ags"])

    # Teilstring
    for key, records in _municipalities.items():
        if norm in key:
            for r in records:
                found.add(r["district_ags"])

    return list(found)


def get_all_states() -> list[dict]:
    """Alle Bundesländer. [{ags, name}] sortiert nach Name."""
    return sorted(
        [{"ags": k, "name": v} for k, v in _states.items()],
        key=lambda x: x["name"]
    )


def get_districts_for_state(state_ags: str) -> list[dict]:
    """Alle Kreise eines Bundeslands. [{ags, name}] sortiert nach Name."""
    return sorted(
        [
            {"ags": k, "name": v["name"]}
            for k, v in _districts.items()
            if v["state_ags"] == state_ags
        ],
        key=lambda x: x["name"]
    )


def get_municipalities_for_district(district_ags: str) -> list[dict]:
    """Alle Gemeinden eines Kreises. [{ags12, name}] sortiert nach Name."""
    results = []
    for records in _municipalities.values():
        for r in records:
            if r["district_ags"] == district_ags:
                results.append({"ags12": r["ags12"], "name": r["name"]})
    return sorted(results, key=lambda x: x["name"])


def district_name(ags5: str) -> str:
    """AGS-Kreis → lesbarer Name. Fallback: AGS selbst."""
    if ags5 in _districts:
        return _districts[ags5]["name"]
    return ags5


def state_name(ags2: str) -> str:
    """AGS-Bundesland → lesbarer Name."""
    return _states.get(ags2, ags2)


def get_state_districts(state_ags: str) -> list[str]:
    """Alle Kreis-AGS (5-stellig) für ein Bundesland. Für nina_poller."""
    return [k for k, v in _districts.items() if v["state_ags"] == state_ags]
