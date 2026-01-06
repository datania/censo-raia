.PHONY: setup data upload clean help

help:
	@echo "Available targets:"
	@echo "  setup  - Install dependencies using uv"
	@echo "  data   - Download RAIA census data"
	@echo "  upload - Upload dataset to HuggingFace"
	@echo "  clean  - Remove downloaded files"
	@echo "  help   - Show this help message"

.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

setup: .uv
	uv sync

data: .uv
	uv run censo_raia.py

clean:
	rm -rf data/

upload:
	uvx --from "huggingface_hub[hf_xet]" hf upload-large-folder \
		--token=${HUGGINGFACE_TOKEN} \
		--repo-type dataset \
		--num-workers 4 \
		datania/censo-raia data/
