# Greenfield Interview Guide

This guide provides a structured heuristic for gathering requirements and planning new projects from scratch.

## 1. The Golden Questions
Ask these sequentially. Do not ask more than 2 at a time.

1.  **Purpose**: "What is the core problem this project solves? Who is the primary user?"
2.  **Scope**: "What are the 3-5 critical 'must-have' features for the MVP?"
3.  **Constraints**: "Are there specific tech stacks, external APIs, or legacy systems we MUST integrate with?"
4.  **Assumptions**: "What are we assuming about the environment (OS, Shell, User skill)?"

## 2. Extraction Heuristic
From the user's answers, immediately extract and draft:
- **Glossary**: Build `.agent/brain/specs/00-GLOSSARY.md`.
- **Architecture**: Draft `.agent/brain/specs/01-ARCHITECTURE.md`.

## 3. Recommended Loop
1. Interview User.
2. Summarize findings.
3. Get approval.
4. Generate Initial Specs.
5. Create First Ticket (S1).
