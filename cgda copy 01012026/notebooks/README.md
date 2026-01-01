## CGDA notebooks

- `LLM_pipeline_debug.ipynb`: Inspect **System → Process → Output** for your grievance data:
  - CSV input (client format or standard CGDA CSV)
  - Preprocessing (header mapping + date parsing + canonical payload)
  - LLM output (Gemini JSON + validated fields)

### How to run

From `cgda/`:

```bash
cd notebooks
jupyter notebook
```

### Notes

- The notebook imports code from `cgda/backend/` directly (no FastAPI server needed).
- You need `pandas` installed in your Python environment for dataframe views.


