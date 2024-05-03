from fastapi import APIRouter, Depends
from schemas import UserBase, UserDisplay
from sqlalchemy.orm import Session
from db.database import get_db
from db import db_user
from typing import List

router = APIRouter(
    prefix='/user',
    tags=['user']
)

# Create User
@router.post('/', response_model=UserDisplay)
def createUser(request: UserBase, db: Session = Depends(get_db)):
    return db_user.create_user(db, request)

# Read All User
@router.get('/', response_model=List[UserDisplay])
def read_all_users(db: Session = Depends(get_db)):
    return db_user.get_all_users(db)

#Update User
@router.post('/{id}/update')
def updateUser(id: int, request: UserBase, db: Session = Depends(get_db)):
    return db_user.update_user(db, id, request)