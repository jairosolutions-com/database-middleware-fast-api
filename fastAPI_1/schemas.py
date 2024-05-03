from pydantic import BaseModel

class UserBase(BaseModel):
    username: str
    email: str
    password: str
    
class UserDisplay(BaseModel):
    username: str
    email: str
    createdAt: str
    modifiedAt: str
    class Config(): #importante ni siya para ma auto convert ang class/ always used for display
        from_attributes = True