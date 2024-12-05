from sqlalchemy import Column, Integer, String, Date, Float, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# Create a base class for declarative models
Base = declarative_base()

class DimProject(Base):
    """
    Dimension table for projects
    """
    __tablename__ = 'dim_project'
    
    id = Column(Integer, primary_key=True)
    client_name = Column(String, nullable=False)
    project_name = Column(String, nullable=False)
    
    # Relationship with fact table
    tasks = relationship("FactTaskTracking", back_populates="project")

class DimEmployee(Base):
    """
    Dimension table for employees
    """
    __tablename__ = 'dim_employee'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    role = Column(String)
    
    # Relationship with fact table
    tasks = relationship("FactTaskTracking", back_populates="employee")

class FactTaskTracking(Base):
    """
    Fact table for task tracking
    """
    __tablename__ = 'fact_task_tracking'
    
    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('dim_project.id'))
    employee_id = Column(Integer, ForeignKey('dim_employee.id'))
    date = Column(Date)
    hours = Column(Float)
    billable = Column(Boolean)
    task_description = Column(String)
    
    # Relationships
    project = relationship("DimProject", back_populates="tasks")
    employee = relationship("DimEmployee", back_populates="tasks")