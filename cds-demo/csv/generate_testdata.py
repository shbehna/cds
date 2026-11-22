"""Generates the synthetic data set of the REQ data product.

The data set illustrates the quality service. Each file has 200 rows: 170 valid rows,
20 rows failing a warning check of the contracts and 10 rows failing an error check
(85 %, 10 % and 5 %). Each failing row fails a single check, chosen to illustrate the
kinds of checks: required columns, primary keys, enumerations, patterns, ranges,
foreign keys, value domains and the custom rules of the contracts.

The rows reference each other as in the register: the establishments, names, mergers
and continuations belong to enterprises of the entreprise file, and the codes belong to
the value domains of the domaine_valeur file, except in the rows illustrating the
foreign key and value domain checks. The enterprises with errors are not referenced.

The files are committed in cds-demo/csv/testdata, in the folder of their data product
(donnees_qc/req), named after the files of the register (Entreprise_testdata.csv for
Entreprise.csv). The transformations read them instead of
the real files when the runtime parameter synthetic_data is true; the deployment uploads
them next to the real files. The generation is deterministic: regenerate the files,
from the root of the repository, after changing this script:

    uv run python cds-demo/csv/generate_testdata.py
"""

from __future__ import annotations

import csv
import io
import random
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

TESTDATA = Path(__file__).with_name("testdata")
REQ = "donnees_qc.req"
# The data products of the data set: every table of each has a synthetic file. The other
# data products load their published files in every environment.
DATA_PRODUCTS = (REQ,)
SUFFIX = "_testdata"
SEED = 2026
ROWS = 200
# The last day of the data set: the dates of the rows are not after it.
LAST_DAY = date(2025, 12, 31)

# Name of the primary key check of the contracts.
PRIMARY_KEY = "primary_key_is_not_unique"

HEADERS: dict[str, tuple[str, ...]] = {
    "Entreprise.csv": (
        "NEQ",
        "IND_FAIL",
        "DAT_IMMAT",
        "COD_REGIM_JURI",
        "COD_INTVAL_EMPLO_QUE",
        "DAT_CESS_PREVU",
        "COD_STAT_IMMAT",
        "COD_FORME_JURI",
        "DAT_STAT_IMMAT",
        "COD_REGIM_JURI_CONSTI",
        "DAT_DEPO_DECLR",
        "AN_DECL",
        "AN_PROD",
        "DAT_LIMIT_PROD",
        "AN_PROD_PRE",
        "DAT_LIMIT_PROD_PRE",
        "DAT_MAJ_INDEX_NOM",
        "COD_ACT_ECON_CAE",
        "NO_ACT_ECON_ASSUJ",
        "DESC_ACT_ECON_ASSUJ",
        "COD_ACT_ECON_CAE2",
        "NO_ACT_ECON_ASSUJ2",
        "DESC_ACT_ECON_ASSUJ2",
        "NOM_LOCLT_CONSTI",
        "DAT_CONSTI",
        "IND_CONVEN_UNMN_ACTNR",
        "IND_RET_TOUT_POUVR",
        "IND_LIMIT_RESP",
        "DAT_DEB_RESP",
        "DAT_FIN_RESP",
        "OBJET_SOC",
        "NO_MTR_VOLONT",
        "ADR_DOMCL_ADR_DISP",
        "ADR_DOMCL_LIGN1_ADR",
        "ADR_DOMCL_LIGN2_ADR",
        "ADR_DOMCL_LIGN3_ADR",
        "ADR_DOMCL_LIGN4_ADR",
    ),
    "Etablissements.csv": (
        "NEQ",
        "NO_SUF_ETAB",
        "IND_ETAB_PRINC",
        "IND_SALON_BRONZ",
        "IND_VENTE_TABAC_DETL",
        "IND_DISP",
        "LIGN1_ADR",
        "LIGN2_ADR",
        "LIGN3_ADR",
        "LIGN4_ADR",
        "COD_ACT_ECON",
        "DESC_ACT_ECON_ETAB",
        "NO_ACT_ECON_ETAB",
        "COD_ACT_ECON2",
        "DESC_ACT_ECON_ETAB2",
        "NO_ACT_ECON_ETAB2",
        "NOM_ETAB",
    ),
    "Nom.csv": (
        "NEQ",
        "NOM_ASSUJ",
        "NOM_ASSUJ_LANG_ETRNG",
        "STAT_NOM",
        "TYP_NOM_ASSUJ",
        "DAT_INIT_NOM_ASSUJ",
        "DAT_FIN_NOM_ASSUJ",
    ),
    "FusionScissions.csv": (
        "NEQ",
        "NEQ_ASSUJ_REL",
        "DENOMN_SOC",
        "COD_RELA_ASSUJ",
        "DAT_EFCTVT",
        "IND_DISP",
        "LIGN1_ADR",
        "LIGN2_ADR",
        "LIGN3_ADR",
        "LIGN4_ADR",
    ),
    "ContinuationsTransformations.csv": (
        "NEQ",
        "COD_TYP_CHANG",
        "COD_REGIM_JURI",
        "AUTR_REGIM_JURI",
        "NOM_LOCLT",
        "DAT_EFCTVT",
    ),
    "DomaineValeur.csv": ("TYP_DOM_VAL", "COD_DOM_VAL", "VAL_DOM_FRAN"),
}

# Prefix of the enterprise numbers of the data set. The register does not use it (its
# numbers start with 114 to 118, 224 to 228, 334 to 338 and 881 to 888): no synthetic row
# has the number of a real enterprise.
NEQ_PREFIX = "999"
# Enterprise numbers matching the pattern of the NEQ, but absent from the entreprise file.
UNKNOWN_NEQS = tuple(f"{NEQ_PREFIX}0099{number:03d}" for number in range(1, 7))
# Codes absent from the value domains.
UNKNOWN_REGIMES = ("009", "099", "777", "888", "999")
UNKNOWN_ACTIVITIES = ("0000", "1234", "4321", "9998", "9999")

Row = dict[str, str]


@dataclass(frozen=True)
class Activity:
    """An economic activity (ACT_ECON domain), with the words naming the enterprises."""

    code: str
    noun: str
    english: str
    description: str


ACTIVITIES = (
    Activity("6013", "BOULANGERIE", "BAKERY", "BOULANGERIE ET PÂTISSERIE"),
    Activity("9212", "RESTAURANT", "RESTAURANT", "RESTAURANT"),
    Activity("9211", "BISTRO", "BISTRO", "RESTAURANT AVEC PERMIS D'ALCOOL"),
    Activity("7721", "SERVICES INFORMATIQUES", "COMPUTER SERVICES", "SERVICES INFORMATIQUES"),
    Activity("4011", "CONSTRUCTION", "CONSTRUCTION", "CONSTRUCTION RÉSIDENTIELLE"),
    Activity("4241", "PLOMBERIE", "PLUMBING", "PLOMBERIE"),
    Activity("4231", "MAÇONNERIE", "MASONRY", "MAÇONNERIE"),
    Activity("4561", "TRANSPORT", "TRUCKING", "CAMIONNAGE"),
    Activity("7215", "GESTION", "HOLDINGS", "SOCIÉTÉ DE PORTEFEUILLE"),
    Activity("7730", "COMPTABILITÉ", "ACCOUNTING", "SERVICES DE COMPTABILITÉ"),
    Activity("7752", "GÉNIE-CONSEIL", "ENGINEERING", "BUREAU D'INGÉNIEURS"),
    Activity("7751", "ARCHITECTURE", "ARCHITECTURE", "BUREAU D'ARCHITECTES"),
    Activity("7771", "CONSEIL", "CONSULTING", "CONSEILLERS EN GESTION"),
    Activity("7612", "IMMOBILIER", "REAL ESTATE", "COURTAGE IMMOBILIER"),
    Activity("7611", "ASSURANCES", "INSURANCE", "AGENCE D'ASSURANCES"),
    Activity("7741", "PUBLICITÉ", "ADVERTISING", "AGENCE DE PUBLICITÉ"),
    Activity("6012", "ÉPICERIE", "GROCERY", "ÉPICERIE"),
    Activity("6351", "GARAGE", "GARAGE", "RÉPARATION AUTOMOBILE"),
    Activity("6531", "QUINCAILLERIE", "HARDWARE", "QUINCAILLERIE"),
    Activity("6511", "LIBRAIRIE", "BOOKSTORE", "LIBRAIRIE ET PAPETERIE"),
    Activity("6541", "SPORTS", "SPORTS", "ARTICLES DE SPORT"),
    Activity("9713", "COIFFURE", "HAIR SALON", "SALON DE COIFFURE"),
    Activity("8641", "GARDERIE", "DAYCARE", "GARDERIE"),
    Activity("9111", "AUBERGE", "INN", "HÉBERGEMENT"),
    Activity("9631", "PRODUCTIONS", "PRODUCTIONS", "AGENCE DE SPECTACLES"),
    Activity("0164", "ÉRABLIÈRE", "MAPLE GROVE", "ÉRABLIÈRE"),
    Activity("0151", "VERGER", "ORCHARD", "CULTURE DES FRUITS"),
    Activity("2611", "ÉBÉNISTERIE", "FURNITURE", "FABRICATION DE MEUBLES"),
)

# Words of the names of the enterprises.
WORDS = (
    "AMÉTHYSTE",
    "ARPENT",
    "AURORE",
    "BORÉAL",
    "BOUCLIER",
    "BOUSSOLE",
    "BRUME",
    "CARIBOU",
    "CASCADE",
    "CÈDRE",
    "CORMORAN",
    "DUNE",
    "ÉPINETTE",
    "ESTUAIRE",
    "ÉTOILE",
    "FALAISE",
    "FJORD",
    "GIVRE",
    "GRANIT",
    "GRÈVE",
    "HÉRON",
    "HORIZON",
    "HUARD",
    "ÎLOT",
    "JASPE",
    "LYNX",
    "MÉLÈZE",
    "NORDET",
    "ORIGNAL",
    "OUTARDE",
    "PHARE",
    "PRAIRIE",
    "QUARTZ",
    "RIVAGE",
    "SAULE",
    "SOLSTICE",
    "TOUNDRA",
    "VERGLAS",
    "ZÉNITH",
)

# Cities, with the forward sortation areas of their postal codes, and their weights.
CITIES = (
    ("Montréal", ("H1A", "H2J", "H2X", "H3B", "H4R"), 30),
    ("Québec", ("G1K", "G1R", "G2E"), 15),
    ("Laval", ("H7N", "H7T"), 8),
    ("Gatineau", ("J8T", "J9H"), 6),
    ("Longueuil", ("J4H", "J4K"), 6),
    ("Sherbrooke", ("J1H", "J1L"), 5),
    ("Saguenay", ("G7H", "G7X"), 4),
    ("Lévis", ("G6V", "G6X"), 4),
    ("Trois-Rivières", ("G8T", "G9A"), 4),
    ("Rimouski", ("G5L",), 3),
    ("Drummondville", ("J2B",), 3),
    ("Granby", ("J2G",), 3),
    ("Saint-Jérôme", ("J7Z",), 3),
    ("Rouyn-Noranda", ("J9X",), 3),
    ("Sept-Îles", ("G4R",), 3),
)
STREETS = (
    "rue Principale",
    "rue Notre-Dame",
    "boul. des Laurentides",
    "av. du Parc",
    "rue des Érables",
    "ch. du Lac",
    "rue Saint-Joseph",
    "boul. de la Rive",
    "rue de la Gare",
    "av. des Pins",
    "rue du Moulin",
    "rang Saint-Louis",
)
# Letters of the postal codes.
POSTAL_LETTERS = "ABCEGHJKLMNPRSTVWXYZ"

# Legal forms (FORM_JURI domain), with their weights.
FORMS = (
    ("CIE", 55),
    ("IND", 10),
    ("APE", 10),
    ("SENC", 6),
    ("SEC", 4),
    ("SENCRL", 3),
    ("COP", 4),
    ("ASS", 4),
    ("SOC", 4),
)
# Legal forms of the enterprises constituted under a law.
CONSTITUTED = ("CIE", "APE", "COP", "SOC")
# Partnerships, which may have a limited liability.
PARTNERSHIPS = ("SENC", "SENCRL", "SEC")
OBJECTS = (
    "Regrouper les producteurs de la région et promouvoir leurs produits.",
    "Offrir des activités sportives et culturelles aux jeunes du quartier.",
    "Soutenir les personnes aînées dans leurs démarches.",
    "Promouvoir la conservation des milieux naturels.",
)
OTHER_LAWS = (
    "Loi canadienne sur les sociétés par actions",
    "Business Corporations Act (Ontario)",
    "Loi sur les corporations commerciales (Nouveau-Brunswick)",
)

# Values of the value domains (DomaineValeur.csv): a subset of those of the register.
DOMAINS: tuple[tuple[str, str, str], ...] = (
    ("FORM_JURI", "APE", "Personne morale sans but lucratif"),
    ("FORM_JURI", "ASS", "Association"),
    ("FORM_JURI", "AU", "Autre"),
    ("FORM_JURI", "AUG", "Groupement de personnes"),
    ("FORM_JURI", "CIE", "Société par actions ou compagnie"),
    ("FORM_JURI", "COP", "Coopérative"),
    ("FORM_JURI", "IND", "Entreprise individuelle"),
    ("FORM_JURI", "MUT", "Mutuelle d'assurance"),
    ("FORM_JURI", "SEC", "Société en commandite"),
    ("FORM_JURI", "SENC", "Société en nom collectif"),
    ("FORM_JURI", "SENCRL", "Société en nom collectif à responsabilité limitée"),
    ("FORM_JURI", "SEP", "Société en participation"),
    ("FORM_JURI", "SMU", "Société de secours mutuel"),
    ("FORM_JURI", "SOC", "Société non constituée au Québec"),
    ("FORM_JURI", "SYC", "Syndicat de copropriété"),
    ("FORM_JURI", "SYP", "Syndicat professionnel"),
    ("INTVAL_EMPLO_QUE", "A", "De 1 à 5"),
    ("INTVAL_EMPLO_QUE", "B", "De 6 à 10"),
    ("INTVAL_EMPLO_QUE", "C", "De 11 à 25"),
    ("INTVAL_EMPLO_QUE", "D", "De 26 à 49"),
    ("INTVAL_EMPLO_QUE", "E", "De 50 à 99"),
    ("INTVAL_EMPLO_QUE", "F", "De 100 à 249"),
    ("INTVAL_EMPLO_QUE", "G", "De 250 à 499"),
    ("INTVAL_EMPLO_QUE", "H", "De 500 à 749"),
    ("INTVAL_EMPLO_QUE", "I", "De 750 à 999"),
    ("INTVAL_EMPLO_QUE", "J", "De 1000 à 2499"),
    ("INTVAL_EMPLO_QUE", "K", "De 2500 à 4999"),
    ("INTVAL_EMPLO_QUE", "L", "Plus de 5000"),
    ("INTVAL_EMPLO_QUE", "N", "Non déclaré"),
    ("INTVAL_EMPLO_QUE", "O", "Aucun"),
    ("INTVAL_EMPLO_QUE", "P", "Non déclaré"),
    ("PAYS", "CDN", "Canada"),
    ("PAYS", "EU", "États-unis"),
    ("PAYS", "FR", "France"),
    ("PAYS", "GB", "Royaume-Uni"),
    ("PAYS", "MX", "Mexique"),
    ("RELA_ASSUJ", "CO", "Conversion"),
    ("RELA_ASSUJ", "FO", "Fusion ordinaire"),
    ("RELA_ASSUJ", "FS", "Fusion simplifiée"),
    ("RELA_ASSUJ", "IN", "Intégration"),
    ("RELA_ASSUJ", "RP", "Remplacé par"),
    ("RELA_ASSUJ", "SC", "Scission"),
    ("STAT_IMMAT", "AI", "Avis d'intention de constitution"),
    ("STAT_IMMAT", "IM", "Immatriculée"),
    ("STAT_IMMAT", "NI", "Non immatriculée"),
    ("STAT_IMMAT", "RD", "Radiée sur demande"),
    ("STAT_IMMAT", "RO", "Radiée d'office"),
    ("STAT_IMMAT", "RX", "Radiée d'office (article 59)"),
    ("STAT_NOM", "A", "Antérieur"),
    ("STAT_NOM", "F", "Futur"),
    ("STAT_NOM", "R", "Réservé"),
    ("STAT_NOM", "V", "En vigueur"),
    ("TYP_CHANG", "CONTN", "Continuation"),
    ("TYP_CHANG", "TRANS", "Transformation"),
    ("TYP_NOM", "A", "Autre nom"),
    ("TYP_NOM", "M", "Denomination sociale"),
    ("TYP_NOM", "N", "Nom"),
    ("TYP_NOM", "V", "Version du nom"),
    ("TYP_RUE", "AUT", "Autoroute"),
    ("TYP_RUE", "AVE", "Avenue"),
    ("TYP_RUE", "BOUL", "Boulevard"),
    ("TYP_RUE", "CAR", "Carrefour"),
    ("TYP_RUE", "CH", "Chemin"),
    ("TYP_RUE", "PARC", "Parc"),
    ("TYP_RUE", "PL", "Place"),
    ("TYP_RUE", "RANG", "Rang"),
    ("TYP_RUE", "RTE", "Route"),
    ("TYP_RUE", "RUE", "Rue"),
    ("REGIM_JURI", "011", "QUÉBEC : Loi sur les assurances (RLRQ, C. A-32)"),
    ("REGIM_JURI", "016", "QUÉBEC : Loi sur les sociétés d'exploration minière (RLRQ, C. S-26)"),
    ("REGIM_JURI", "021", "QUÉBEC : Loi sur les compagnies, Partie 1 (RLRQ, C. C-38)"),
    ("REGIM_JURI", "022", "QUÉBEC : Loi sur les compagnies, Partie 2 (RLRQ, C. C-38)"),
    ("REGIM_JURI", "023", "QUÉBEC : Loi sur les compagnies, Partie 3 (RLRQ, C. C-38)"),
    ("REGIM_JURI", "024", "QUÉBEC : Loi sur les sociétés par actions (RLRQ, C. S-31.1)"),
    ("REGIM_JURI", "025", "QUÉBEC : Loi sur les caisses d'épargne et de crédit (RLRQ, C. C-4)"),
    ("REGIM_JURI", "030", "QUÉBEC : Loi sur les coopératives (RLRQ, C. C-67.2)"),
    ("REGIM_JURI", "031", "QUÉBEC : Loi sur les sociétés d'entraide économique (RLRQ, C. S-25.1)"),
    ("REGIM_JURI", "032", "Autre loi étrangère"),
    ("REGIM_JURI", "036", "QUÉBEC : Loi sur les associations coopératives (RLRQ, C. A-24)"),
    ("REGIM_JURI", "042", "QUÉBEC : Loi sur les sociétés de fiducie et les sociétés d'épargne (RLRQ, C. S-29.01)"),
    ("REGIM_JURI", "051", "QUÉBEC : Loi sur les coopératives de services financiers (RLRQ, C. C-67.3)"),
    ("REGIM_JURI", "052", "QUÉBEC : Loi du Québec à caractère privé"),
    ("REGIM_JURI", "053", "QUÉBEC : Loi du Québec à caractère public"),
    ("REGIM_JURI", "101", "CANADA : Loi du Canada à caractère privé"),
    ("REGIM_JURI", "102", "CANADA : Loi du Canada à caractère public"),
    ("REGIM_JURI", "103", "ONTARIO : Loi sur les sociétés par actions, L.R.O. 1990, c. B.16"),
    ("REGIM_JURI", "104", "CANADA: Loi canadienne sur les sociétés par actions, L.R.C. (1985), c. C-44"),
    ("REGIM_JURI", "400", "QUÉBEC : Loi sur les compagnies partie 1A, RLRQ, C. C-38"),
    ("PROV_ETAT", "AB", "Alberta"),
    ("PROV_ETAT", "BC", "Colombie-Britannique"),
    ("PROV_ETAT", "MB", "Manitoba"),
    ("PROV_ETAT", "NB", "Nouveau-Brunswick"),
    ("PROV_ETAT", "NL", "Terre-Neuve-et-Labrador"),
    ("PROV_ETAT", "NS", "Nouvelle-Écosse"),
    ("PROV_ETAT", "NT", "Territoires du Nord-Ouest"),
    ("PROV_ETAT", "NU", "Nunavut"),
    ("PROV_ETAT", "PE", "Île-du-Prince-Édouard"),
    ("PROV_ETAT", "SK", "Saskatchewan"),
    ("PROV_ETAT", "YT", "Territoire du Yukon"),
    ("ACT_ECON", "1", "Activités indéterminées et imprécises"),
    ("ACT_ECON", "2", "Information non déclarée"),
    ("ACT_ECON", "3", "Aucune"),
    ("ACT_ECON", "0100", "Agriculture"),
    ("ACT_ECON", "0139", "Autres grandes cultures"),
    ("ACT_ECON", "0151", "Culture des fruits"),
    ("ACT_ECON", "0164", "Érablière"),
    ("ACT_ECON", "0222", "Services de poudrage et de pulvérisation des cultures (sauf par aéronef)"),
    ("ACT_ECON", "0410", "Exploitation forestière"),
    ("ACT_ECON", "0629", "Autres mines de minerais non métalliques (sauf le charbon)"),
    ("ACT_ECON", "0921", "Forage à forfait (sauf pétrole et gaz)"),
    ("ACT_ECON", "1060", "Industrie des aliments pour animaux"),
    ("ACT_ECON", "1130", "Industrie de la bière"),
    ("ACT_ECON", "1620", "Industrie des tuyaux et raccords de tuyauterie en matière plastique"),
    ("ACT_ECON", "1714", "Industrie des accessoires pour bottes et chaussures"),
    ("ACT_ECON", "1932", "Industrie des sacs et poches en matière textile"),
    ("ACT_ECON", "2442", "Industrie des vêtements de sport pour femmes"),
    ("ACT_ECON", "2510", "Industrie du bois de sciage et des bardeaux"),
    ("ACT_ECON", "2591", "Industrie de la préservation du bois"),
    ("ACT_ECON", "2611", "Industrie des meubles de maison en bois"),
    ("ACT_ECON", "2700", "Industrie du papier et des produits en papier"),
    ("ACT_ECON", "2800", "Imprimerie, édition et industrie connexes"),
    ("ACT_ECON", "2919", "Autres industries sidérurgiques"),
    ("ACT_ECON", "3020", "Industries des produits de construction en métal"),
    ("ACT_ECON", "3062", "Industrie des matrices, des moules et des outils tranchants et à profiler en métal"),
    ("ACT_ECON", "3192", "Industrie de l'équipement de manutention"),
    ("ACT_ECON", "3255", "Industrie des roues et des freins pour véhicules automobiles"),
    ("ACT_ECON", "3331", "Industrie des appareils d'éclairage pour immeubles"),
    ("ACT_ECON", "3390", "Autres industries de produits électriques"),
    ("ACT_ECON", "3560", "Industries du verre et des articles en verre"),
    ("ACT_ECON", "3700", "Industries chimiques"),
    ("ACT_ECON", "3791", "Industrie des encres d'imprimerie"),
    ("ACT_ECON", "3971", "Industrie des enseignes et étalages"),
    ("ACT_ECON", "4011", "Promotion et construction de maisons individuelles"),
    ("ACT_ECON", "4030", "Travaux de génie"),
    ("ACT_ECON", "4220", "Travaux de charpenterie et travaux connexes"),
    ("ACT_ECON", "4231", "Travaux de maçonnerie"),
    ("ACT_ECON", "4241", "Plomberie"),
    (
        "ACT_ECON",
        "4243",
        "Installation de systèmes de chauffage par fluide caloporteur et de systèmes de climatisation",
    ),
    ("ACT_ECON", "4277", "Pose de revêtements de plancher en bois dur"),
    ("ACT_ECON", "4520", "Services relatifs aux transports aériens"),
    ("ACT_ECON", "4555", "Agences d'expédition maritime"),
    ("ACT_ECON", "4561", "Camionnage de marchandises ordinaires"),
    ("ACT_ECON", "4591", "Entretien des routes, rues et ponts"),
    ("ACT_ECON", "4814", "Câblovision"),
    ("ACT_ECON", "4999", "Autres services publics"),
    ("ACT_ECON", "5216", "Commerce de gros de fruits et légumes frais"),
    ("ACT_ECON", "5314", "Commerce de gros de chaussures"),
    ("ACT_ECON", "5500", "Commerces de gros de véhicules automobiles, pièces et accessoires"),
    ("ACT_ECON", "5631", "Commerce de gros de bois de construction, de contre-plaqués et de bois travaillé"),
    (
        "ACT_ECON",
        "5749",
        "Autres types de commerce de machines, matériel et fournitures électriques et électroniques",
    ),
    ("ACT_ECON", "5931", "Commerce de gros d'aliments pour animaux"),
    ("ACT_ECON", "5993", "Commerce de gros de produits forestiers"),
    ("ACT_ECON", "6012", "Épiceries (sauf supermarchés)"),
    ("ACT_ECON", "6013", "Boulangeries et pâtisseries"),
    ("ACT_ECON", "6111", "Commerce de détail de chaussures pour femmes"),
    (
        "ACT_ECON",
        "6212",
        "Commerce de détail de meubles de maison (sans appareils ménagers ni accessoires d'ameublement)",
    ),
    ("ACT_ECON", "6321", "Commerce de détail de roulottes motorisées et de roulottes de voyage"),
    ("ACT_ECON", "6351", "Garages (réparations générales)"),
    ("ACT_ECON", "6392", "Commerce de détail de radios pour véhicules automobiles"),
    ("ACT_ECON", "6511", "Librairies et papeteries"),
    ("ACT_ECON", "6531", "Quincailleries"),
    ("ACT_ECON", "6541", "Commerce de détail d'articles de sport"),
    ("ACT_ECON", "6552", "Commerce de détail de disques et de bandes magnétiques"),
    ("ACT_ECON", "6598", "Commerce de détail de maisons mobiles"),
    ("ACT_ECON", "7052", "Caisses centrales d'épargne et de crédit"),
    ("ACT_ECON", "7215", "Sociétés de portefeuille (holdings)"),
    ("ACT_ECON", "7221", "Sociétés de placement hypothécaire"),
    ("ACT_ECON", "7413", "Courtiers en marchandises"),
    ("ACT_ECON", "7611", "Agences d'assurances"),
    ("ACT_ECON", "7612", "Agences ou courtiers immobiliers"),
    ("ACT_ECON", "7614", "Évaluateurs agréés, estimateurs agréés"),
    ("ACT_ECON", "7721", "Services d'informatique"),
    ("ACT_ECON", "7730", "Services de comptabilité et de tenue de livres"),
    ("ACT_ECON", "7741", "Agences de publicité"),
    ("ACT_ECON", "7751", "Bureaux d'architectes"),
    ("ACT_ECON", "7752", "Bureaux d'ingénieurs"),
    ("ACT_ECON", "7756", "Bureaux de chimistes ou de chimistes professionnels"),
    ("ACT_ECON", "7771", "Bureaux de conseillers en gestion"),
    ("ACT_ECON", "7796", "Services de reproduction"),
    ("ACT_ECON", "8142", "Aide internationale"),
    ("ACT_ECON", "8176", "Gestion de la recherche"),
    ("ACT_ECON", "8262", "Gestion des services sociaux"),
    ("ACT_ECON", "8350", "Services administratifs généraux"),
    ("ACT_ECON", "8411", "Organismes internationaux et autres organismes extraterritoriaux"),
    ("ACT_ECON", "8611", "Centres hospitaliers de soins de courte durée"),
    ("ACT_ECON", "8641", "Garderies pour enfants"),
    ("ACT_ECON", "8648", "Services d'aide de nature affective ou psychologique"),
    ("ACT_ECON", "8666", "Cabinets d'optométristes"),
    ("ACT_ECON", "8685", "Banques de sang"),
    ("ACT_ECON", "9111", "Hôtels et auberges routières"),
    ("ACT_ECON", "9131", "Terrains de camping et parcs à roulottes"),
    ("ACT_ECON", "9211", "Restaurants avec permis d'alcool"),
    ("ACT_ECON", "9212", "Restaurants sans permis d'alcool"),
    ("ACT_ECON", "9620", "Projection de films cinématographiques"),
    ("ACT_ECON", "9631", "Agences de spectacles et d'artistes"),
    ("ACT_ECON", "9659", "Autres clubs sportifs et services de loisirs"),
    ("ACT_ECON", "9713", "Salons de coiffure et de beauté pour hommes et femmes"),
    ("ACT_ECON", "9721", "Blanchissage ou nettoyage à sec mécanisés"),
    ("ACT_ECON", "9811", "Organisations religieuses"),
    ("ACT_ECON", "9920", "Services de location d'automobiles et de camions"),
)


@dataclass
class SourceFile:
    """The rows of a file of the data set, and the checks each row is expected to fail."""

    name: str
    # The data product of the file (<group>.<data product>): the file is in its folder.
    data_product: str
    rows: list[Row] = field(default_factory=list)
    # Names of the checks failed by each row.
    expected: list[set[str]] = field(default_factory=list)
    # Rows failing an error check.
    errors: set[int] = field(default_factory=set)

    @property
    def file_name(self) -> str:
        """Name of the file of the data set: the name of the file of the register, with a suffix."""
        stem, extension = self.name.rsplit(".", 1)
        return f"{stem}{SUFFIX}.{extension}"

    @property
    def path(self) -> str:
        """Path of the file in the data set: the folder of its data product, <group>/<data product>."""
        return f"{self.data_product.replace('.', '/')}/{self.file_name}"

    def add(self, values: Row) -> int:
        """Adds a valid row and returns its index; the columns not given are empty."""
        unknown = set(values) - set(HEADERS[self.name])
        if unknown:
            raise ValueError(f"{self.name} has no column {', '.join(sorted(unknown))}")
        self.rows.append({column: values.get(column, "") for column in HEADERS[self.name]})
        self.expected.append(set())
        return len(self.rows) - 1

    def warn(self, index: int, check: str, values: Row) -> None:
        """Changes a row so that it fails a warning check."""
        self.rows[index].update(values)
        self.expected[index].add(check)

    def fail(self, index: int, check: str, values: Row) -> None:
        """Changes a row so that it fails an error check."""
        self.warn(index, check, values)
        self.errors.add(index)

    def duplicate_key(self, source: int, target: int, columns: Sequence[str]) -> None:
        """Copies the key of a row to another one: both fail the primary key check."""
        self.fail(target, PRIMARY_KEY, {column: self.rows[source][column] for column in columns})
        self.fail(source, PRIMARY_KEY, {})

    def render(self) -> str:
        """Returns the content of the file: UTF-8 CSV with a byte order mark, as the register publishes it."""
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(HEADERS[self.name])
        writer.writerows([row[column] for column in HEADERS[self.name]] for row in self.rows)
        return "\ufeff" + buffer.getvalue()


class Picker:
    """Draws distinct rows of a file, so that each changed row fails a single check."""

    def __init__(self, rng: random.Random, source: SourceFile) -> None:
        self.rng = rng
        self.source = source
        self.available = list(range(len(source.rows)))

    def take(self, count: int, where: Callable[[Row], bool] | None = None) -> list[int]:
        candidates = [index for index in self.available if where is None or where(self.source.rows[index])]
        chosen = self.rng.sample(candidates, count)
        self.available = [index for index in self.available if index not in chosen]
        return chosen

    def pair(self, where: Callable[[Row], bool] | None = None) -> tuple[int, int]:
        source, target = self.take(2, where)
        return source, target


@dataclass(frozen=True)
class Address:
    line_1: str
    city: str
    postal_code: str

    @property
    def line_2(self) -> str:
        return f"{self.city} (Québec)"


@dataclass(frozen=True)
class Enterprise:
    """An enterprise of the data set, and the values its other rows share."""

    neq: str
    form: str
    regime: str
    registered: date
    name: str
    english_name: str
    activity: Activity
    address: Address


def iso(day: date) -> str:
    return day.isoformat()


def later(rng: random.Random, day: date, minimum: int, maximum: int) -> date:
    """Returns a day after another one, not after the last day of the data set."""
    found = day + timedelta(days=rng.randrange(minimum, maximum))
    if found <= LAST_DAY:
        return found
    # Too late: a day between the two, rather than many rows on the last day.
    return day + timedelta(days=rng.randrange(max((LAST_DAY - day).days, 0) + 1))


def a_century_later(value: str) -> str:
    """Returns a date a hundred years later: a typing error in the year, such as 2114 for 2014."""
    day = date.fromisoformat(value)
    # 2100 is not a leap year: February 29 becomes February 28.
    return iso(day.replace(year=day.year + 100, day=min(day.day, 28) if day.month == 2 else day.day))


def address(rng: random.Random) -> Address:
    city, areas, _ = rng.choices(CITIES, weights=[weight for _, _, weight in CITIES])[0]
    postal_code = rng.choice(areas) + str(rng.randrange(10)) + rng.choice(POSTAL_LETTERS) + str(rng.randrange(10))
    return Address(f"{rng.randrange(10, 9999)} {rng.choice(STREETS)}", city, postal_code)


def numbered_name(rng: random.Random, number: int) -> str:
    """Returns the name of a numbered company. The register numbers them from 9000-0000: these
    names start with 0, so that they are not those of real companies."""
    return f"0{number:03d}-{rng.randrange(1000, 10000)} QUÉBEC INC."


def enterprise_names(rng: random.Random, form: str, activity: Activity, word: str, number: int) -> tuple[str, str]:
    """Returns the name of an enterprise, and its English version (empty if none)."""
    if form == "CIE":
        if rng.random() < 0.15:
            return numbered_name(rng, number), ""
        english = f"{word} {activity.english} INC." if rng.random() < 0.3 else ""
        return f"{activity.noun} {word} INC.", english
    names = {
        "IND": f"{activity.noun} {word}",
        "SENC": f"{activity.noun} {word} S.E.N.C.",
        "SENCRL": f"{activity.noun} {word} S.E.N.C.R.L.",
        "SEC": f"{activity.noun} {word} S.E.C.",
        "APE": f"FONDATION {word}",
        "COP": f"COOPÉRATIVE {activity.noun} {word}",
        "ASS": f"REGROUPEMENT {word}",
        "SOC": f"{word} {activity.english} CORPORATION",
    }
    return names[form], ""


def enterprises(rng: random.Random) -> list[Enterprise]:
    combinations = [(activity, word) for activity in ACTIVITIES for word in WORDS]
    rng.shuffle(combinations)
    found = []
    for number in range(1, ROWS + 1):
        activity, word = combinations[number]
        form = rng.choices([form for form, _ in FORMS], weights=[weight for _, weight in FORMS])[0]
        regime = {
            "CIE": rng.choices(("024", "104"), weights=(80, 20))[0],
            "APE": "023",
            "COP": "030",
            "SOC": "103",
        }.get(form, "")
        name, english_name = enterprise_names(rng, form, activity, word, number)
        registered = date(1995, 1, 1) + timedelta(days=rng.randrange(30 * 365))
        found.append(
            Enterprise(
                f"{NEQ_PREFIX}00{number:05d}", form, regime, registered, name, english_name, activity, address(rng)
            )
        )
    return found


def enterprise_row(rng: random.Random, enterprise: Enterprise) -> Row:
    registered = enterprise.registered
    status = rng.choices(("IM", "RO", "RD"), weights=(80, 10, 10))[0]
    if status == "IM":
        deposit = date(2025, 6, 1) + timedelta(days=rng.randrange(450))
        status_date = deposit
    else:
        deposit = later(rng, registered, 365, 3000)
        status_date = later(rng, deposit, 30, 700)
    anniversary = (registered.month, min(registered.day, 28))
    row = {
        "NEQ": enterprise.neq,
        "IND_FAIL": rng.choices(("N", ""), weights=(70, 30))[0],
        "DAT_IMMAT": iso(registered),
        "COD_REGIM_JURI": enterprise.regime,
        "COD_INTVAL_EMPLO_QUE": rng.choices("ABCDEFNO", weights=(30, 15, 10, 5, 3, 2, 15, 20))[0],
        "COD_STAT_IMMAT": status,
        "COD_FORME_JURI": enterprise.form,
        "DAT_STAT_IMMAT": iso(status_date),
        "DAT_DEPO_DECLR": iso(deposit),
        "AN_DECL": str(max(registered.year, deposit.year - 1)),
        "AN_PROD": "2026",
        "DAT_LIMIT_PROD": iso(date(2026, *anniversary)),
        "AN_PROD_PRE": "2025",
        "DAT_LIMIT_PROD_PRE": iso(date(2025, *anniversary)),
        "DAT_MAJ_INDEX_NOM": (
            f"{iso(registered)} {rng.randrange(8, 18):02d}:{rng.randrange(60):02d}:{rng.randrange(60):02d}"
        ),
        "COD_ACT_ECON_CAE": enterprise.activity.code,
        "NO_ACT_ECON_ASSUJ": "1",
        "DESC_ACT_ECON_ASSUJ": enterprise.activity.description,
        "ADR_DOMCL_ADR_DISP": "N",
        "ADR_DOMCL_LIGN1_ADR": enterprise.address.line_1,
        "ADR_DOMCL_LIGN2_ADR": enterprise.address.line_2,
        "ADR_DOMCL_LIGN4_ADR": enterprise.address.postal_code,
    }
    if rng.random() < 0.2:
        second = rng.choice([activity for activity in ACTIVITIES if activity != enterprise.activity])
        row |= {"COD_ACT_ECON_CAE2": second.code, "NO_ACT_ECON_ASSUJ2": "2", "DESC_ACT_ECON_ASSUJ2": second.description}
    if enterprise.form in CONSTITUTED:
        before_2011 = enterprise.form == "CIE" and enterprise.regime == "024" and registered.year < 2011
        row |= {
            "COD_REGIM_JURI_CONSTI": "400" if before_2011 else enterprise.regime,
            "NOM_LOCLT_CONSTI": "TORONTO" if enterprise.form == "SOC" else enterprise.address.city.upper(),
            "DAT_CONSTI": iso(registered - timedelta(days=rng.randrange(15))),
        }
    if enterprise.form == "CIE":
        agreement = rng.random() < 0.1
        row |= {"IND_CONVEN_UNMN_ACTNR": "O" if agreement else "N", "IND_RET_TOUT_POUVR": "N" if agreement else ""}
    if enterprise.form in PARTNERSHIPS:
        limited = enterprise.form == "SENCRL"
        row["IND_LIMIT_RESP"] = "O" if limited else "N"
        if limited:
            start = later(rng, registered, 0, 60)
            row["DAT_DEB_RESP"] = iso(start)
            if rng.random() < 0.3:
                row["DAT_FIN_RESP"] = iso(later(rng, start, 365, 3000))
    if enterprise.form in ("APE", "ASS"):
        row["OBJET_SOC"] = rng.choice(OBJECTS)
    return row


def entreprise_file(rng: random.Random, found: Sequence[Enterprise]) -> SourceFile:
    source = SourceFile("Entreprise.csv", REQ)
    for enterprise in found:
        source.add(enterprise_row(rng, enterprise))
    picker = Picker(rng, source)
    # Errors.
    for index, neq in zip(
        picker.take(3), (f"{NEQ_PREFIX}000012", f"{NEQ_PREFIX}00A0045", f"{NEQ_PREFIX}0-00078"), strict=True
    ):
        source.fail(index, "numero_entreprise_does_not_match_pattern", {"NEQ": neq})
    for index, form in zip(picker.take(2), ("INC", "LTEE"), strict=True):
        source.fail(index, "code_forme_juridique_is_not_in_enum", {"COD_FORME_JURI": form})
    [index] = picker.take(1)
    source.fail(index, "code_statut_immatriculation_is_not_in_enum", {"COD_STAT_IMMAT": "AC"})
    [index] = picker.take(1)
    source.fail(index, "code_intervalle_employes_is_not_in_enum", {"COD_INTVAL_EMPLO_QUE": "Z"})
    [index] = picker.take(1)
    source.fail(index, "date_immatriculation_is_null", {"DAT_IMMAT": ""})
    source.duplicate_key(*picker.pair(), ["NEQ"])
    # Warnings.
    for index in picker.take(5):
        source.warn(index, "code_regime_juridique_is_not_allowed", {"COD_REGIM_JURI": rng.choice(UNKNOWN_REGIMES)})
    for index in picker.take(4):
        regime = rng.choice(UNKNOWN_REGIMES)
        source.warn(index, "code_regime_juridique_constitution_is_not_allowed", {"COD_REGIM_JURI_CONSTI": regime})
    for index in picker.take(4):
        activity = rng.choice(UNKNOWN_ACTIVITIES)
        source.warn(index, "code_premiere_activite_is_not_allowed", {"COD_ACT_ECON_CAE": activity})
    for index in picker.take(3):
        values = {"COD_ACT_ECON_CAE2": rng.choice(UNKNOWN_ACTIVITIES), "NO_ACT_ECON_ASSUJ2": "2"}
        values["DESC_ACT_ECON_ASSUJ2"] = "AUTRE ACTIVITÉ"
        source.warn(index, "code_deuxieme_activite_is_not_allowed", values)
    for index in picker.take(4, lambda row: row["COD_FORME_JURI"] in PARTNERSHIPS):
        # The limited liability ends before it starts.
        start = later(rng, date.fromisoformat(source.rows[index]["DAT_IMMAT"]), 30, 400)
        end = start - timedelta(days=rng.randrange(30, 300))
        values = {"IND_LIMIT_RESP": "O", "DAT_DEB_RESP": iso(start), "DAT_FIN_RESP": iso(end)}
        source.warn(index, "periode_responsabilite_limitee_valide", values)
    return source


def establishment_row(enterprise: Enterprise, number: int, place: Address, name: str) -> Row:
    activity = enterprise.activity
    return {
        "NEQ": enterprise.neq,
        "NO_SUF_ETAB": str(number),
        "IND_ETAB_PRINC": "O" if number == 1 else "N",
        "IND_SALON_BRONZ": "O" if activity.code == "9713" and number == 1 else "N",
        "IND_VENTE_TABAC_DETL": "O" if activity.code == "6012" else "N",
        "IND_DISP": "N",
        "LIGN1_ADR": place.line_1,
        "LIGN2_ADR": place.line_2,
        "LIGN4_ADR": place.postal_code,
        "COD_ACT_ECON": activity.code,
        "DESC_ACT_ECON_ETAB": activity.description,
        "NO_ACT_ECON_ETAB": "1",
        "NOM_ETAB": name,
    }


def etablissement_file(rng: random.Random, clean: Sequence[Enterprise]) -> SourceFile:
    source = SourceFile("Etablissements.csv", REQ)
    counts = {enterprise.neq: 1 for enterprise in clean}
    for enterprise in clean:
        source.add(establishment_row(enterprise, 1, enterprise.address, enterprise.name))
    companies = [enterprise for enterprise in clean if enterprise.form == "CIE"]
    while len(source.rows) < ROWS:
        enterprise = rng.choice(companies)
        counts[enterprise.neq] += 1
        place = address(rng)
        name = f"{enterprise.name} - {place.city.upper()}"
        source.add(establishment_row(enterprise, counts[enterprise.neq], place, name))
    picker = Picker(rng, source)
    # Errors.
    for index, neq in zip(picker.take(3), UNKNOWN_NEQS, strict=False):
        source.fail(index, "numero_entreprise_not_in_entreprise", {"NEQ": neq})
    # The main establishments: their numbers are distinct, and remain so once changed.
    for index in picker.take(2, lambda row: row["NO_SUF_ETAB"] == "1"):
        source.fail(index, "numero_etablissement_is_out_of_range", {"NO_SUF_ETAB": "0"})
    for index in picker.take(2):
        # An indicator neither O nor N: the transformation makes it null.
        source.fail(index, "indicateur_salon_bronzage_is_null", {"IND_SALON_BRONZ": "X"})
    [index] = picker.take(1)
    source.fail(index, "numero_etablissement_is_null", {"NO_SUF_ETAB": ""})
    source.duplicate_key(*picker.pair(), ["NEQ", "NO_SUF_ETAB"])
    # Warnings.
    for index in picker.take(10):
        source.warn(index, "code_premiere_activite_is_not_allowed", {"COD_ACT_ECON": rng.choice(UNKNOWN_ACTIVITIES)})
    for index in picker.take(10):
        values = {"COD_ACT_ECON2": rng.choice(UNKNOWN_ACTIVITIES), "NO_ACT_ECON_ETAB2": "2"}
        values["DESC_ACT_ECON_ETAB2"] = "AUTRE ACTIVITÉ"
        source.warn(index, "code_deuxieme_activite_is_not_allowed", values)
    return source


NAME_KEY = ("NEQ", "NOM_ASSUJ", "NOM_ASSUJ_LANG_ETRNG", "TYP_NOM_ASSUJ", "DAT_INIT_NOM_ASSUJ")


def nom_file(rng: random.Random, clean: Sequence[Enterprise]) -> SourceFile:
    source = SourceFile("Nom.csv", REQ)
    current: dict[str, int] = {}
    for enterprise in clean:
        current[enterprise.neq] = source.add(
            {
                "NEQ": enterprise.neq,
                "NOM_ASSUJ": enterprise.name,
                "NOM_ASSUJ_LANG_ETRNG": enterprise.english_name,
                "STAT_NOM": "V",
                "TYP_NOM_ASSUJ": "M" if enterprise.form in CONSTITUTED else "N",
                "DAT_INIT_NOM_ASSUJ": iso(enterprise.registered),
            }
        )
    renamed: set[str] = set()
    while len(source.rows) < ROWS:
        enterprise = rng.choice(clean)
        registered = enterprise.registered
        if enterprise.form == "CIE" and enterprise.neq not in renamed and not enterprise.name.startswith("0"):
            # A numbered company renamed: its former name is withdrawn when the current one is registered.
            renamed.add(enterprise.neq)
            renaming = later(rng, registered, 30, 1500)
            source.rows[current[enterprise.neq]]["DAT_INIT_NOM_ASSUJ"] = iso(renaming)
            source.add(
                {
                    "NEQ": enterprise.neq,
                    "NOM_ASSUJ": numbered_name(rng, rng.randrange(100, 1000)),
                    "STAT_NOM": "A",
                    "TYP_NOM_ASSUJ": "M",
                    "DAT_INIT_NOM_ASSUJ": iso(registered),
                    "DAT_FIN_NOM_ASSUJ": iso(renaming),
                }
            )
        else:
            # Another name under which the enterprise does business.
            other = f"{enterprise.activity.noun} {rng.choice(WORDS)} {enterprise.address.city.upper()}"
            key = (enterprise.neq, other)
            if any((row["NEQ"], row["NOM_ASSUJ"]) == key for row in source.rows):
                continue
            source.add(
                {
                    "NEQ": enterprise.neq,
                    "NOM_ASSUJ": other,
                    "STAT_NOM": "V",
                    "TYP_NOM_ASSUJ": "A",
                    "DAT_INIT_NOM_ASSUJ": iso(later(rng, registered, 30, 2000)),
                }
            )

    def in_force(row: Row) -> bool:
        return row["STAT_NOM"] == "V"

    picker = Picker(rng, source)
    # Errors.
    for index, neq in zip(picker.take(3, in_force), UNKNOWN_NEQS, strict=False):
        source.fail(index, "numero_entreprise_not_in_entreprise", {"NEQ": neq})
    for index in picker.take(2, in_force):
        source.fail(index, "code_statut_nom_is_not_in_enum", {"STAT_NOM": "Z"})
    for index in picker.take(2, in_force):
        source.fail(index, "nom_is_null", {"NOM_ASSUJ": ""})
    [index] = picker.take(1, in_force)
    source.fail(index, "code_type_nom_is_not_in_enum", {"TYP_NOM_ASSUJ": "X"})
    source.duplicate_key(*picker.pair(in_force), NAME_KEY)
    # Warnings.
    for index in picker.take(10, in_force):
        registered = date.fromisoformat(source.rows[index]["DAT_INIT_NOM_ASSUJ"])
        withdrawn = registered - timedelta(days=rng.randrange(30, 900))
        source.warn(index, "periode_nom_valide", {"STAT_NOM": "A", "DAT_FIN_NOM_ASSUJ": iso(withdrawn)})
    for index in picker.take(10, in_force):
        source.warn(index, "nom_anterieur_avec_date_retrait", {"STAT_NOM": "A"})
    return source


def fusion_file(rng: random.Random, clean: Sequence[Enterprise]) -> SourceFile:
    source = SourceFile("FusionScissions.csv", REQ)
    companies = [enterprise for enterprise in clean if enterprise.form == "CIE"]
    while len(source.rows) < ROWS:
        resulting = rng.choice(companies)
        code = rng.choices(("FO", "FS", "SC", "CO", "IN", "RP"), weights=(45, 25, 10, 8, 6, 6))[0]
        count = min(rng.randrange(1, 4) if code in ("FO", "FS") else 1, ROWS - len(source.rows))
        linked = rng.sample([enterprise for enterprise in clean if enterprise != resulting], count)
        effective = later(rng, max([resulting.registered, *(enterprise.registered for enterprise in linked)]), 30, 2000)
        for enterprise in linked:
            row = {"NEQ": resulting.neq, "COD_RELA_ASSUJ": code, "DAT_EFCTVT": iso(effective), "IND_DISP": "N"}
            if rng.random() < 0.15:
                # An enterprise merged before it was registered: only its name is known.
                row["DENOMN_SOC"] = f"{enterprise.activity.noun} {rng.choice(WORDS)} ({rng.randrange(1960, 1995)}) LTÉE"
            else:
                place = enterprise.address
                row |= {
                    "NEQ_ASSUJ_REL": enterprise.neq,
                    "DENOMN_SOC": enterprise.name,
                    "LIGN1_ADR": place.line_1,
                    "LIGN2_ADR": place.line_2,
                    "LIGN4_ADR": place.postal_code,
                }
            source.add(row)
    picker = Picker(rng, source)
    names = {enterprise.neq: enterprise.name for enterprise in clean}
    # Errors.
    for index, neq in zip(picker.take(3), UNKNOWN_NEQS, strict=False):
        source.fail(index, "numero_entreprise_not_in_entreprise", {"NEQ": neq})
    for index, neq in zip(picker.take(3, lambda row: row["NEQ_ASSUJ_REL"] != ""), UNKNOWN_NEQS[3:], strict=False):
        source.fail(index, "numero_entreprise_liee_not_in_entreprise", {"NEQ_ASSUJ_REL": neq})
    for index in picker.take(2):
        source.fail(index, "code_relation_is_not_in_enum", {"COD_RELA_ASSUJ": "ZZ"})
    for index in picker.take(2):
        source.fail(index, "date_effet_is_null", {"DAT_EFCTVT": ""})
    # Warnings.
    for index in picker.take(10):
        neq = source.rows[index]["NEQ"]
        source.warn(index, "entreprise_liee_distincte", {"NEQ_ASSUJ_REL": neq, "DENOMN_SOC": names[neq]})
    for index in picker.take(10):
        source.warn(index, "date_effet_passee", {"DAT_EFCTVT": a_century_later(source.rows[index]["DAT_EFCTVT"])})
    return source


CHANGE_KEY = ("NEQ", "COD_TYP_CHANG", "COD_REGIM_JURI", "DAT_EFCTVT")


def continuation_file(rng: random.Random, clean: Sequence[Enterprise]) -> SourceFile:
    source = SourceFile("ContinuationsTransformations.csv", REQ)

    def change(enterprise: Enterprise, kind: str, after: date) -> Row:
        return {
            "NEQ": enterprise.neq,
            "COD_TYP_CHANG": kind,
            "COD_REGIM_JURI": enterprise.regime if rng.random() < 0.5 else "",
            "AUTR_REGIM_JURI": rng.choice(OTHER_LAWS) if kind == "CONTN" and rng.random() < 0.15 else "",
            "NOM_LOCLT": enterprise.address.city if rng.random() < 0.4 else "",
            "DAT_EFCTVT": iso(later(rng, after, 200, 4000)),
        }

    first: dict[str, Row] = {}
    for enterprise in clean:
        first[enterprise.neq] = change(
            enterprise, rng.choices(("CONTN", "TRANS"), weights=(80, 20))[0], enterprise.registered
        )
        source.add(first[enterprise.neq])
    for enterprise in rng.sample([enterprise for enterprise in clean if enterprise.form == "CIE"], ROWS - len(clean)):
        # A second change, after the first one.
        source.add(change(enterprise, "TRANS", date.fromisoformat(first[enterprise.neq]["DAT_EFCTVT"])))
    keys = [tuple(row[column] for column in CHANGE_KEY) for row in source.rows]
    if len(set(keys)) != len(keys):
        raise ValueError("Two changes of the valid rows have the same key: change the seed.")
    picker = Picker(rng, source)
    # Errors.
    for index, neq in zip(picker.take(3), UNKNOWN_NEQS, strict=False):
        source.fail(index, "numero_entreprise_not_in_entreprise", {"NEQ": neq})
    for index in picker.take(2):
        source.fail(index, "code_type_changement_is_not_in_enum", {"COD_TYP_CHANG": "FUSN"})
    for index in picker.take(2):
        source.fail(index, "date_effet_is_null", {"DAT_EFCTVT": ""})
    [index] = picker.take(1)
    source.fail(index, "numero_entreprise_is_null", {"NEQ": ""})
    source.duplicate_key(*picker.pair(), CHANGE_KEY)
    # Warnings.
    for index in picker.take(12):
        source.warn(index, "code_regime_juridique_is_not_allowed", {"COD_REGIM_JURI": rng.choice(UNKNOWN_REGIMES)})
    for index in picker.take(8):
        source.warn(index, "date_effet_passee", {"DAT_EFCTVT": a_century_later(source.rows[index]["DAT_EFCTVT"])})
    return source


def domaine_file(rng: random.Random, files: Iterable[SourceFile]) -> SourceFile:
    source = SourceFile("DomaineValeur.csv", REQ)
    for domain, code, description in DOMAINS:
        source.add({"TYP_DOM_VAL": domain, "COD_DOM_VAL": code, "VAL_DOM_FRAN": description})
    # The codes used by the other files remain in their domains: only the others are changed.
    code_columns = (
        "COD_REGIM_JURI",
        "COD_REGIM_JURI_CONSTI",
        "COD_ACT_ECON_CAE",
        "COD_ACT_ECON_CAE2",
        "COD_ACT_ECON",
        "COD_ACT_ECON2",
    )
    used = {row[column] for other in files for row in other.rows for column in code_columns if column in row}

    def unused(row: Row) -> bool:
        return row["TYP_DOM_VAL"] in ("ACT_ECON", "PAYS", "PROV_ETAT", "TYP_RUE") and row["COD_DOM_VAL"] not in used

    picker = Picker(rng, source)
    # Errors.
    for index in picker.take(4, unused):
        source.fail(index, "description_is_null", {"VAL_DOM_FRAN": ""})
    for index in picker.take(2, unused):
        source.fail(index, "code_is_null", {"COD_DOM_VAL": ""})
    for index in picker.take(2, unused):
        source.fail(index, "domaine_is_null", {"TYP_DOM_VAL": ""})
    source.duplicate_key(*picker.pair(unused), ["TYP_DOM_VAL", "COD_DOM_VAL"])
    # Warnings.
    for index in picker.take(10, unused):
        code = source.rows[index]["COD_DOM_VAL"]
        source.warn(index, "code_sans_espaces", {"COD_DOM_VAL": rng.choice((f" {code}", f"{code} "))})
    for index in picker.take(10, unused):
        source.warn(index, "domaine_en_majuscules", {"TYP_DOM_VAL": source.rows[index]["TYP_DOM_VAL"].lower()})
    return source


def generate() -> list[SourceFile]:
    """Returns the files of the data set."""
    rng = random.Random(SEED)
    found = enterprises(rng)
    entreprise = entreprise_file(rng, found)
    # The enterprises in error are not referenced by the other files.
    clean = [enterprise for index, enterprise in enumerate(found) if index not in entreprise.errors]
    files = [
        entreprise,
        etablissement_file(rng, clean),
        nom_file(rng, clean),
        fusion_file(rng, clean),
        continuation_file(rng, clean),
    ]
    return [*files, domaine_file(rng, files)]


def render() -> dict[str, str]:
    """Returns the content of each file of the data set, by path in the data set."""
    return {source.path: source.render() for source in generate()}


def main() -> None:
    for name, content in render().items():
        path = TESTDATA / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as file:
            file.write(content)
        print(f"Wrote {path}")


if __name__ == "__main__":
    main()
