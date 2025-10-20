### install

python -m venv .venv
uv python install 3.11.9
uv venv -p 3.11.9 .venv
source .venv/bin/activate
uv pip install -r requirement.txt

### Run

uvicorn app.main:app --reload
