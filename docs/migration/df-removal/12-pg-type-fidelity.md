---
title: Postgres Type System Fidelity
nav_section: migration
sidebar_position: 12
summary: Authoritative spec for Basin's logical Postgres type system after DataFusion removal — typmod enforcement, NUMERIC, arrays, domains, coercion, NULL semantics, and wire-protocol type reporting.
tags:
  - migration
  - types
  - postgres-compatibility
  - coercion
  - wire-protocol
---

<!-- STATUS: IN PROGRESS — research underway. Sections filled incrementally. -->

# Postgres Type System Fidelity

## 1. Scope and relationship to the IR design

## 2. Inventory: Basin's type support today

### 2.1 Type registry

### 2.2 Divergences from Postgres

## 3. Target logical type system

### 3.1 typmod enforcement

### 3.2 NUMERIC / arbitrary precision

### 3.3 Arrays as first-class Postgres types

### 3.4 Domains, composites, ENUMs, ranges

### 3.5 The `unknown` type and late literal resolution

## 4. Coercion and cast fidelity

### 4.1 Postgres's three cast categories

### 4.2 Function and operator overload resolution

### 4.3 How the Basin planner should implement it

## 5. NULL and edge semantics

## 6. Wire-protocol type reporting

## 7. Prioritized fidelity gap table

## 8. Open questions
