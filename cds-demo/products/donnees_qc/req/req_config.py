"""Data product of the enterprise register of Québec (Registraire des entreprises, REQ)."""

from cds import DataProductConfig, ScdSettings

config = DataProductConfig(
    name="req",
    # The comment of the schema of the data product.
    description=(
        "Registre des entreprises du Québec : les entreprises, leurs noms, leurs établissements, leurs fusions, "
        "scissions, continuations et transformations, tirés du jeu de données ouvertes du Registraire des "
        "entreprises."
    ),
    # The register publishes a few rows sharing the same key, which the quality service
    # reports. The history keeps one row per key instead of failing.
    scd=ScdSettings(on_duplicate_keys="deduplicate"),
    # Merged under every contract of the data product, over the defaults of the group. A
    # contract defining slaProperties or support replaces the whole list.
    contract_defaults={
        "domain": "registre_entreprises",
        "dataProduct": "donnees_qc.req",
        "status": "active",
        # The service levels (cds.sla): the deployment schedules the job with the
        # frequency, and alerts on the latency and the error rate (cds-demo/manifest); the
        # scd service applies the retention to the history tables.
        "slaProperties": [
            {
                "property": "frequency",
                "value": 15,
                "unit": "d",
                "scheduler": "cron",
                "schedule": "0 6 1,15 * *",
                "description": (
                    "Le registre est publié deux fois par mois : les tables sont chargées le 1er et le 15 de "
                    "chaque mois, à 6 h."
                ),
            },
            {
                "property": "latency",
                "value": 18,
                "unit": "d",
                "scheduler": "cron",
                "schedule": "0 7 * * *",
                "description": (
                    "Le dernier chargement des tables date de moins de 18 jours : jusqu'à 17 jours séparent deux "
                    "chargements, du 15 d'un mois de 31 jours au 1er du mois suivant. Vérifié chaque jour à 7 h."
                ),
            },
            {
                "property": "errorRate",
                "value": 1,
                "unit": "percent",
                "scheduler": "cron",
                # After the load of 6 am: the error rate only changes when the tables are loaded.
                "schedule": "0 12 1,15 * *",
                "description": (
                    "Au plus 1 % des lignes de chaque table ont une erreur de qualité. Vérifié après chaque "
                    "chargement, le 1er et le 15 de chaque mois à midi."
                ),
            },
            {
                "property": "retention",
                "value": 10,
                "unit": "y",
                "description": "L'historique garde les versions remplacées pendant 10 ans.",
            },
        ],
        # The alerts of the service levels are sent to the email channels. The address of
        # the repository is fictitious: the deployment adds those of the ALERT_EMAILS secret.
        "support": [
            {
                "channel": "donnees-qc-req",
                "tool": "email",
                "scope": "notifications",
                "url": "mailto:donnees-qc-req@example.com",
                "description": "Alertes des niveaux de service du produit de données.",
            }
        ],
        "description": {
            "usage": "Analyse des entreprises immatriculées au Québec.",
            "limitations": (
                "Source : Registraire des entreprises du Québec, jeu de données ouvertes publié sur Données "
                "Québec sous la licence CC BY-NC-SA 4.0. Les données sont mises à jour deux fois par mois et "
                "peuvent accuser un retard sur le registre."
            ),
        },
    },
)
