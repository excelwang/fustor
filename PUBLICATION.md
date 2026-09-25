# Publication and deployment boundary

This source snapshot excludes private deployment records, credentials, execution
traces, generated evidence, local assistant state and unrelated presentation assets.
Existing local development history is retained separately; publication is not a
deployment or a real-provider acceptance certificate.

Documentation and local fixtures use RFC 5737 example addresses. They are not
connection defaults for a running installation. Review all example configuration
before use. Supply credentials by the documented private environment/file
references, never in Git. Do not use historical deployment scripts on a running
service without separately reviewing their targets, backups and write scope.

The solution is composed of the existing software owners: Coox handles deterministic
task execution and Trace; Fustor owns data views and data access; Capanix supplies
app-neutral resource capabilities; Dashboard is read-only observation.
Agentic Map is the solution name, not an additional service.

A host-policy change requires a reviewed private installation configuration and
revalidation before a future deployment. This publication does not update services,
credentials, imported data, source registrations or scheduled jobs.
