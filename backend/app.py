import os
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient

app = Flask(__name__)
CORS(app) 

# --- Connexion à MongoDB ---
# "mongo" est le nom du service dans docker-compose.yml
MONGO_URI = "mongodb://mongo_user:mongo_pass@mongo:27017/?authSource=admin"
client = MongoClient(MONGO_URI)

db = client.kbo_data
collection = db.companies

@app.route("/api/companies")
def get_companies():
    """
    Récupère toutes les entreprises de la collection.
    """
    try:
        companies_cursor = collection.find({}, {'_id': 0})
        companies_list = list(companies_cursor)
        
        return jsonify(companies_list)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0')