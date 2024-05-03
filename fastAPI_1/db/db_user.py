from sqlalchemy.orm.session import Session
from schemas import UserBase
from db.models import DbUser
from db.hash import Hash
from datetime import datetime
from fastapi import HTTPException, status
from middleware1 import test1
import importlib


# creating user
def create_user(db: Session, request: UserBase):
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_user = DbUser(
        username = request.username,
        email = request.email,
        password = Hash.bcrypt(request.password),
        createdAt=current_datetime,
        modifiedAt=current_datetime,
    )
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    print('created')
    importlib.reload(test1)
    print('created')

    return new_user

# reading users
def get_all_users(db: Session):
    users = db.query(DbUser).all() 
    return users

def update_user(db: Session, id: int, request: UserBase):
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user = db.query(DbUser).filter(DbUser.id == id)
    if not user.first():
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail=f'User with id {id} not found :(')
    user.update({
        DbUser.username: request.username,
        DbUser.email: request.email,
        DbUser.password: Hash.bcrypt(request.password),
        DbUser.modifiedAt: current_datetime
    })
    db.commit()
    importlib.reload(test1)
    return 'ok'