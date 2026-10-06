# Architecture

```mermaid
flowchart LR
    A[Patients CSV] --> B[Bronze: Raw]
    C[Claims CSV] --> B
    B --> D[Silver: Cleansed + Enriched]
    D --> E[Gold: Healthcare KPIs]
    E --> F[Patient Cost Analytics]
    E --> G[Provider Performance]
    E --> H[Diagnosis Analytics]
    E --> I[Monthly Claims Trends]
```

## Azure production mapping

- ADLS Gen2: Bronze/Silver/Gold storage
- Azure Databricks: PySpark transformation and orchestration
- Unity Catalog: governance, permissions and lineage
- Azure Data Factory / Databricks Workflows: scheduling
- Power BI: Gold-layer reporting
- Microsoft Purview: catalog, lineage and governance
