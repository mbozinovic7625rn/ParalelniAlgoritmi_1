[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/RWsCQ5La)
[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=21247010&assignment_repo_type=AssignmentRepo)

# Paralelni algoritmi

## Domaći zadatak broj 1

### Mihailo Božinović RN 76/2025

### Luka Ljubičić SI 98/2024

Nije uradjeno:

- Bonus zadatak za ProcessPool

Pitanja za AI:

- Napravi mi primere dag fajlova koji sadrže i fail i ispituju Planera i RafThreadPool,
- Da li je potreban lock u Graf klasi (pozitivan odgovor),
- Da li je moguće uraditi Future klasu preko semafora ili condition-a? (Odgovor je bio pozitivan. Za semafore je overkill dok je za condition-e moguće uraditi),
- Kako izvršiti shell i py proces iz naše akcije u python skripti? (Odgovor je bio preko subprocess-a i exec funkcije).

# DAG Task Scheduler — Parallel Algorithms, Assignment 1

First homework assignment for the *Paralelni algoritmi* (Parallel Algorithms) course at Računarski fakultet (RAF), Belgrade: a **multithreaded DAG task scheduler** in pure Python — a planner that executes dependency graphs of tasks over a custom thread pool.

## What it does

- **Task graphs as JSON** — each `dag_*.json` file describes nodes with dependencies, actions (shell / Python commands executed via `subprocess`), expected outputs, and resource requirements.
- **Custom thread pool (`RafThreadPool`)** — worker threads coordinated with `Lock`, `Condition`, and `Event` primitives (no `concurrent.futures`).
- **Planner** — tracks node states (`PENDING` → running → done/failed), releases dependents as their dependencies complete, and handles failure propagation through the graph.
- **Resource-aware scheduling** — nodes declare resource needs; the planner synchronizes access so concurrent tasks don't oversubscribe shared resources.
- **Test DAGs** — `dag_1.json` … `dag_7.json` cover normal execution and failure scenarios exercising both the planner and the thread pool.

## Running it

```bash
python prvi_projekat.py   # loads and executes the configured DAG file(s)
```

## Status & notes

- Bonus task (ProcessPool variant) was not implemented.
- AI assistance was used and disclosed per course guidelines — for generating additional test DAG files and for design questions (whether the graph class needs a lock; implementing a Future via conditions; launching shell/Python processes from actions via `subprocess`). All scheduler and thread-pool code was written and debugged by us.

## Authors

Mihailo Božinović (RN 76/2025) · Luka Ljubičić (SI 98/2024) — Računarski fakultet (RAF), Belgrade
