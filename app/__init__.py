from flask import Flask

app = Flask(__name__)

from app.elastic import api_ES
from app.arango import api_AR
from app.data_migrate import api_PG

app.register_blueprint(api_ES)
app.register_blueprint(api_AR)
app.register_blueprint(api_PG)

from . import data_migrate
from . import elastic
from . import arango
