#How to Run

- initiate virtual environment "python -m venv venv"
- activate virtual environment "venv\Scripts\activate"
- change directory to the first fastapi
- run uvicorn main:app --reload on the first api
- change directory to the second fastapi
- run uvicorn main:app --host 127.0.0.1 --port 3000
- open the docs for each
- run the API UI
