.PHONY: setup data test upload clean help

help:
	@echo "Available targets:"
	@echo "  setup  - Install dependencies using uv"
	@echo "  data   - Download RAIA census data"
	@echo "  test   - Run offline download tests"
	@echo "  upload - Upload dataset to HuggingFace"
	@echo "  clean  - Remove downloaded files"
	@echo "  help   - Show this help message"

.uv:
	@uv -V || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

setup: .uv
	uv sync

data: .uv
	uv run censo_raia.py

test: .uv
	uv run --locked python -m unittest discover -s tests -v

clean:
	rm -rf data/

upload:
	uvx --from "huggingface_hub[hf_xet]" hf upload-large-folder \
		--token=${HUGGINGFACE_TOKEN} \
		--repo-type dataset \
		--num-workers 4 \
		datania/censo-raia data/
