"""Pulumi program of the CDS demo: see pipeline.py."""

from pipeline import Settings, deploy

deploy(Settings.from_stack())
