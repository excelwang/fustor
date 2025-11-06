from typing import Optional, Dict
from pydantic import BaseModel, Field
from fustor_common.models import ResponseBase, DatastoreBase

class DatastoreCreate(DatastoreBase):
    pass

class DatastoreUpdate(DatastoreBase):
    pass

class DatastoreResponse(ResponseBase, DatastoreCreate):
    id: int