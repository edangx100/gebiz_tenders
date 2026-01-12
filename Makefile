.PHONY: help fetch chunk extract view-samples normalize import run-all query-agencies query-suppliers query-categories query-requirements query-similar ui clean test typecheck

# Print available make commands and examples.
help:
	@echo "GeBIZ Tender Intelligence Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make fetch           - Download GeBIZ data (use LIMIT=N to limit records)"
	@echo "  make chunk           - Build tender chunks from raw data"
	@echo "  make extract         - Extract entities using GLiNER2 (auto-normalizes)"
	@echo "  make view-samples    - View extraction samples (use COUNT=N, default 3)"
	@echo "  make normalize       - Apply normalization to existing extracted.jsonl"
	@echo "  make import          - Import graph into Neo4j"
	@echo "  make run-all         - Run complete pipeline (use LIMIT=N to limit records)"
	@echo "  make query-agencies  - Query agencies (use MODE=top or AGENCY='name')"
	@echo "  make query-suppliers - Query suppliers (use MODE=top or SUPPLIER='name')"
	@echo "  make query-categories- Query categories (use MODE=all, CATEGORY='name', or CATEGORY_GROUP='name')"
	@echo "  make query-requirements - Find tenders with shared requirements"
	@echo "  make query-similar   - Find similar tenders (requires TENDER='name')"
	@echo "  make ui              - Launch Gradio web interface"
	@echo "  make test            - Run tests"
	@echo "  make typecheck       - Run type checking"
	@echo "  make clean           - Clean generated data files"
	@echo ""
	@echo "Examples:"
	@echo "  make fetch LIMIT=10"
	@echo "  make run-all LIMIT=20"
	@echo "  make view-samples COUNT=5"
	@echo "  make query-agencies MODE=top LIMIT=5"
	@echo "  make query-agencies AGENCY='Ministry of Health'"
	@echo "  make query-suppliers MODE=top LIMIT=5"
	@echo "  make query-suppliers SUPPLIER='Acme Corp'"
	@echo "  make query-categories MODE=all"
	@echo "  make query-categories CATEGORY='IT Services'"
	@echo "  make query-requirements MIN_OVERLAP=2 LIMIT=10"
	@echo "  make query-similar TENDER='Tender XYZ' LIMIT=5"

# Download raw GeBIZ data (optionally limited by LIMIT).
fetch:
	python -m pipeline.fetch $(if $(LIMIT),--limit $(LIMIT))

# Split raw data into smaller tender chunks (optionally limited by LIMIT).
chunk:
	python -m pipeline.chunk $(if $(LIMIT),--limit $(LIMIT))

# Run entity extraction over chunked tenders.
extract:
	python -m pipeline.extract

# Show a few extracted samples (optionally set COUNT).
view-samples:
	python -m pipeline.view_samples $(if $(COUNT),--count $(COUNT))

# Apply normalization to existing extracted data (standalone utility).
normalize:
	python -m pipeline.apply_normalize $(if $(LIMIT),--limit $(LIMIT))

# Import extracted data into Neo4j (optionally limit and/or clear existing graph).
import:
	python -m pipeline.import_graph $(if $(LIMIT),--limit $(LIMIT)) $(if $(CLEAR),--clear)

# Run the full pipeline end-to-end (optionally limited by LIMIT).
run-all:
	python -m pipeline.run_all $(if $(LIMIT),--limit $(LIMIT))

# Query agencies (either top agencies or tenders for a named agency).
query-agencies:
	@if [ "$(MODE)" = "top" ]; then \
		python -m queries.agency --top $(if $(LIMIT),--limit $(LIMIT)); \
	elif [ -n "$(AGENCY)" ]; then \
		python -m queries.agency --agency "$(AGENCY)"; \
	else \
		echo "Error: Specify MODE=top or AGENCY='name'"; \
		echo "Examples:"; \
		echo "  make query-agencies MODE=top LIMIT=5"; \
		echo "  make query-agencies AGENCY='Ministry of Health'"; \
		exit 1; \
	fi

# Query suppliers (either top suppliers or tenders for a named supplier).
query-suppliers:
	@if [ "$(MODE)" = "top" ]; then \
		python -m queries.supplier --top $(if $(LIMIT),--limit $(LIMIT)); \
	elif [ -n "$(SUPPLIER)" ]; then \
		python -m queries.supplier --supplier "$(SUPPLIER)"; \
	else \
		echo "Error: Specify MODE=top or SUPPLIER='name'"; \
		echo "Examples:"; \
		echo "  make query-suppliers MODE=top LIMIT=5"; \
		echo "  make query-suppliers SUPPLIER='Acme Corp'"; \
		exit 1; \
	fi

# Query categories (all categories or a specific category).
query-categories:
	@if [ "$(MODE)" = "all" ]; then \
		python -m queries.category --all $(if $(MAX_TERMS),--max-terms $(MAX_TERMS)); \
	elif [ -n "$(CATEGORY)" ]; then \
		python -m queries.category --category "$(CATEGORY)" $(if $(MAX_TERMS),--max-terms $(MAX_TERMS)); \
	elif [ -n "$(CATEGORY_GROUP)" ]; then \
		python -m queries.category --group "$(CATEGORY_GROUP)" $(if $(MAX_TERMS),--max-terms $(MAX_TERMS)); \
	else \
		echo "Error: Specify MODE=all, CATEGORY='name', or CATEGORY_GROUP='name'"; \
		echo "Examples:"; \
		echo "  make query-categories MODE=all"; \
		echo "  make query-categories CATEGORY='IT Services'"; \
		echo "  make query-categories CATEGORY_GROUP='IT Services & Software'"; \
		exit 1; \
	fi

# Query tenders with shared requirements.
query-requirements:
	python -m queries.requirements $(if $(MIN_OVERLAP),--min-overlap $(MIN_OVERLAP)) $(if $(LIMIT),--limit $(LIMIT))

# Query similar tenders based on shared keywords and requirements.
query-similar:
	@if [ -z "$(TENDER)" ]; then \
		echo "Error: TENDER parameter is required"; \
		echo "Example: make query-similar TENDER='Tender Name' LIMIT=5"; \
		exit 1; \
	fi
	python -m queries.similar --tender "$(TENDER)" $(if $(LIMIT),--limit $(LIMIT)) $(if $(INCLUDE_CATEGORY),--include-category)

# Launch Gradio web interface for interactive exploration.
ui:
	python -m ui.app

# Run the test suite with verbose output.
test:
	pytest -v

# Type-check pipeline code with mypy strict mode.
typecheck:
	mypy pipeline/ --strict --ignore-missing-imports

# Remove generated data and Python cache artifacts.
clean:
	rm -rf data/raw/* data/chunks/* data/extracted/*
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
