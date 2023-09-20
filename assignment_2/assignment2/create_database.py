import os

import mysql.connector as mysql
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()



con = mysql.connect(
    host=os.getenv("MYSQL_HOST"),
    user=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    database=os.getenv("MYSQL_DATABASE"),
)

cursor = con.cursor()

cursor.execute("DROP TABLE IF EXISTS trajectories")
cursor.execute("DROP TABLE IF EXISTS trips")
cursor.execute("DROP TABLE IF EXISTS users")

cursor.execute(
    """CREATE TABLE users (
        user_id INT PRIMARY KEY
    )"""
)

cursor.execute(
    """CREATE TABLE trips (
        trip_id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        FOREIGN KEY (user_id) REFERENCES users(user_id)
    )"""
)
cursor.execute(
    """CREATE TABLE trajectories (
        trajectory_id INT AUTO_INCREMENT PRIMARY KEY,
        trip_id INT,
        latitude FLOAT,
        longitude FLOAT,
        altitude FLOAT,
        days_since_1899 FLOAT,
        FOREIGN KEY (trip_id) REFERENCES trips(trip_id)
    )"""
)

con.commit()


subDir = "./dataset/Data"
user_ids = os.listdir(subDir)

labeled_ids_dir = "./dataset/labeled_ids.txt"
labeled_ids = open(labeled_ids_dir, "r").read().strip().split("\n")

num_users = len(user_ids)
for i in tqdm(range(num_users)):
    user_id = user_ids[i]
    if user_id.startswith("."):
        continue
    try:
        
        cursor.execute("INSERT INTO users (user_id) VALUES (%s)", (int(user_id),))

        path = subDir + "/" + user_id + "/Trajectory"
        fileNames = os.listdir(path)
        for f in fileNames:
            cursor.execute("INSERT INTO trips (user_id) VALUES (%s)", (int(user_id),))
            trip_id = cursor.lastrowid
            trajectory_file_path = path + "/" + f
            with open(trajectory_file_path, "r") as trajectory_file:
                lines = trajectory_file.read().strip().split("\n")[6:]
                for line in lines:
                    data = line.split(",")
                    latitude = float(data[0])
                    longitude = float(data[1])
                    altitude = float(data[3])
                    days_since_1899 = float(data[4])
                    
                    cursor.execute(
                        "INSERT INTO trajectories (trip_id, latitude, longitude, altitude, days_since_1899) VALUES (%s, %s, %s, %s, %s)",
                        (
                            trip_id,
                            latitude,
                            longitude,
                            altitude,
                            days_since_1899,
                        ),
                    )
                
    except Exception as e:
        raise e
        print("error for user id :", user_id)
        continue
    con.commit()
    
con.close()
print("Done!")