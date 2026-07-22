# Upload to GitHub from VS Code

1. Open this folder in VS Code.
2. Open the Source Control panel.
3. Select **Initialize Repository**.
4. Make incremental commits rather than one bulk upload.

Recommended sequence:

```text
chore: initialize project structure and documentation
feat: add Kafka producer consumer validation and DLQ
feat: add Delta Bronze MERGE Silver and Gold
feat: add Great Expectations quality gate
feat: add Chroma BM25 RRF and cross-encoder RAG
feat: add OpenLineage events per pipeline stage
feat: add Airflow DAG and failure-path scripts
docs: add architecture evidence and run instructions
```

5. Create an empty repository in your GitHub account.
6. In the VS Code terminal:

```powershell
git branch -M main
git remote add origin YOUR_REPOSITORY_URL
git push -u origin main
```

Do not publish `.env`, generated Delta data, Chroma indexes, or local runtime logs unless the trainer specifically requests evidence in the repository.
