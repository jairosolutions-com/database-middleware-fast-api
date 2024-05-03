from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table
from sqlalchemy.orm import sessionmaker

app = FastAPI()

# SQLite database URL (change this to your database URL)
DATABASE_URL = "sqlite:///./fastapi_1-practice.db"

# Create a SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Define metadata
metadata = MetaData()

# Define a table model (example)
users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("name", String, index=True),
    Column("age", Integer),
)

# Create all defined tables in the database
metadata.create_all(bind=engine)

# Dependency to get database session
def get_session():
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = Session()
    try:
        yield session
    finally:
        session.close()


# FastAPI endpoint to fetch all data from the 'users' table
@app.get("/get-all-users", response_model=list[dict])
async def get_all_users(session: sessionmaker = get_session):
    # Use a session to query all records in the 'users' table
    with session() as db:
        query = db.query(users).all()
        if not query:
            raise HTTPException(status_code=404, detail="No users found")
        # Convert SQLAlchemy result objects to dictionaries
        user_list = [dict(row) for row in query]
        return user_list
