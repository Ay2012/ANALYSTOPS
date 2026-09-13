# AnalystOps

AnalystOps is currently in Phase One: deterministic source loading and dataset
simulation before any AI or agent workflow is added.

## Phase One Dataset Flow

The first implemented slice reads the UCI Online Retail II source workbook and
splits it into clean country/month submission workbooks.

```text
data/source/online_retail_II.xlsx
  -> src/analystops/datasets/uci_online_retail.py
  -> src/analystops/datasets/splitter.py
  -> data/generated/clean/<month>/<country>_<month>.xlsx
```

The source workbook is treated as read-only input.

## Generate Clean Submissions

Install the project dependencies, then run:

```bash
PYTHONPATH=src python -m analystops.datasets.splitter --overwrite
```

For a small smoke run:

```bash
PYTHONPATH=src python -m analystops.datasets.splitter --max-files 3 --overwrite
```

Generated files are runtime artifacts and are ignored by git.

## Generate Seeded Corruptions

Once clean submissions exist, create seeded corrupted workbooks with:

```bash
PYTHONPATH=src python -m analystops.datasets.corruptions --seed 42 --max-files 1 --overwrite
```

By default this generates every Phase One corruption scenario for each selected
clean submission under `data/generated/corrupted/`.

## Generate Manifests

Generate clean submissions, corrupted submissions, and JSON answer keys with:

```bash
PYTHONPATH=src python -m analystops.datasets.manifests --seed 42 --max-files 1 --overwrite
```

Manifests are written under `data/manifests/`.
