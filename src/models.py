from datetime import datetime
from pydantic import BaseModel, Field
from sqlmodel import Field as SQLField, SQLModel


class ExtractItem(BaseModel):
    parent_link: str = Field(default="", description="The link followed to the current webpage")
    link: str = Field(description="Link to webpage")
    depth: int = Field(ge=0, description="Depth of page")


class RawData(BaseModel):
    link: str = Field(description="Link to webpage")
    parent_link: str = Field(description="The link followed to the current webpage")
    content: str = Field(description="Text content of page")
    scraped_at: datetime = Field(default_factory=datetime.now(), description="Date and Time webpage was scraped at")


class TransformedData(SQLModel, table=True):
    link: str = SQLField(primary_key=True, description="Link to webpage")
    parent_link: str = SQLField(description="The link followed to the current webpage")
    title: str = SQLField(index=True, description="Title of page")
    content: str = SQLField(description="Text content of page")
    num_links: int = SQLField(default=0, gt=0, description="Number of links on page")
    num_h2: int = SQLField(default=0, gt=0, description="Number of H2 headings on page")
    num_refs: int = SQLField(default=0, gt=0, description="Number of references on page")
    word_count: int = SQLField(default=0, gt=0, description="Number of words on page")
    scraped_at: datetime = SQLField(default_factory=datetime.now(), description="Date and Time webpage was scraped at")
    modified_at_utc: datetime = SQLField(
        default_factory=datetime.now(), description="Date and Time webpage was last modified at in UTC timezone"
    )
