---
title: pg_catalog Fidelity
nav_section: migration
sidebar_position: 11
summary: Making pg_catalog and information_schema real queryable relations backed by Basin's own catalog metadata, so psql, pg_dump, ORMs, migration tools and GUI clients work unmodified.
tags:
  - migration
  - datafusion-removal
  - catalog
  - pg_catalog
  - postgres-compatibility
---

# pg_catalog Fidelity

> STATUS: DRAFT IN PROGRESS. Sections are filled in incrementally; claims not yet
> checked against source are marked **UNVERIFIED**.

## 1. What Basin exposes today

## 2. Target design: real pg_catalog relations

## 3. OID allocation

## 4. search_path and schema resolution

## 5. Payoff: which tools this unlocks

## 6. CREATE EXTENSION without dlopen

## 7. LOC estimate and phased order
