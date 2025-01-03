.PHONY: default
default:
	@echo ""


.PHONY: setup
setup:
	if command -v pyenv 1>/dev/null 2>&1; then \
		pyenv install 3.12; \
		pyenv local 3.12; \
		pyenv shell 3.12; \
		python -m pip install --upgrade pip; \
		python -m venv .venv; \
		source .venv/bin/activate; \
		python -m pip install -r requirements.txt; \
	fi


