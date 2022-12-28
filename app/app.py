from fastapi import FastAPI
from pydantic import BaseModel
import pathlib
import os

app = FastAPI()


class MigrationModel(BaseModel):
    city_names: Optional[str]
    district_names: Optional[str]

@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/migrate/")
def create_item(migrationModel: MigrationModel):
    migrate_path = str(pathlib.Path().resolve()) + "/app/spark/migrate.py"
    python_script_command = "python3 " + migrate_path

    if migrationModel.city_names is not None:
        print("Setting city names")
        python_script_command = (
            python_script_command
            + f" --spark-migration-city-filter {migrationModel.city_names}"
        )

    if migrationModel.district_names is not None:
        print("Setting district names")
        python_script_command = (
            python_script_command
            + f" --spark-migration-district-filter {migrationModel.district_names}"
        )

    print("Triggering a spark job with command -> ", python_script_command)
    os.system(python_script_command)

    return migrationModel
