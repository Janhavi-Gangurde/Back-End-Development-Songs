from . import app
import os
import json
import pymongo
from flask import jsonify, request, make_response, abort, url_for  # noqa; F401
from pymongo import MongoClient
from bson import json_util
from pymongo.errors import OperationFailure
import sys

# Load JSON data
SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
json_url = os.path.join(SITE_ROOT, "data", "songs.json")
songs_list: list = json.load(open(json_url))

# Environment variables for MongoDB connection
mongodb_service = os.environ.get('MONGODB_SERVICE')
mongodb_username = os.environ.get('MONGODB_USERNAME')
mongodb_password = os.environ.get('MONGODB_PASSWORD')
mongodb_port = os.environ.get('MONGODB_PORT')

print(f'The value of MONGODB_SERVICE is: {mongodb_service}')

# Check MongoDB service variable
if mongodb_service is None:
    app.logger.error('Missing MongoDB server in the MONGODB_SERVICE variable')
    sys.exit(1)

# Construct MongoDB URL
if mongodb_username and mongodb_password:
    url = f"mongodb://{mongodb_username}:{mongodb_password}@{mongodb_service}"
else:
    url = f"mongodb://{mongodb_service}"

print(f"Connecting to URL: {url}")

# MongoDB client connection
try:
    client = MongoClient(url)
except OperationFailure as e:
    app.logger.error(f"Authentication error: {str(e)}")
    sys.exit(1)

# Define database and collection
db = client.songs
collection = db.songs

# Drop existing data and insert songs from JSON
collection.drop()
collection.insert_many(songs_list)

def parse_json(data):
    return json.loads(json_util.dumps(data))

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify(status="OK"), 200

@app.route("/count", methods=['GET'])
def count():
    """Return length of data in the collection"""
    count = collection.count_documents({})
    return {"count": count}, 200

@app.route("/song", methods=['GET'])
def songs():
    """Return all songs in the collection"""
    songs = list(collection.find({}))
    songs = [parse_json(song) for song in songs]
    return {"songs": songs}, 200

@app.route("/song/<int:id>", methods=['GET'])
def get_song_by_id(id):
    """Return a song by its id"""
    song = collection.find_one({"id": id})
    if not song:
        return {"message": "song with id not found"}, 404
    return jsonify(parse_json(song)), 200

@app.route("/song", methods=["POST"])
def create_song():
    """Add a new song to the collection."""
    song_data = request.get_json()
    if not song_data or 'id' not in song_data:
        return {"message": "Song ID is required"}, 400
    song_id = song_data['id']
    existing_song = db.songs.find_one({"id": song_id})
    if existing_song:
        return {"message": f"song with id {song_id} already present"}, 302
    try:
        insert_result = db.songs.insert_one(song_data)
    except Exception as e:
        app.logger.error(f"Insertion error: {str(e)}")
        return {"message": "An error occurred while inserting the song"}, 500
    return {"inserted_id": str(insert_result.inserted_id)}, 201

@app.route("/song/<int:id>", methods=["PUT"])
def update_song(id):
    """Update an existing song's details."""
    song_data = request.get_json()
    existing_song = db.songs.find_one({"id": id})
    if not existing_song:
        return {"message": "song not found"}, 404
    if all(existing_song.get(key) == value for key, value in song_data.items()):
        return {"message": "song found, but nothing updated"}, 200
    db.songs.update_one({"id": id}, {"$set": song_data})
    updated_song = db.songs.find_one({"id": id})
    return jsonify(parse_json(updated_song)), 201

@app.route("/song/<int:id>", methods=["DELETE"])
def delete_song(id):
    """Delete a song by its id."""
    result = db.songs.delete_one({"id": id})  # Delete the song with the given id
    if result.deleted_count == 0:
        return jsonify({"message": "song not found"}), 404
    return "", 204  # No content if deleted successfully