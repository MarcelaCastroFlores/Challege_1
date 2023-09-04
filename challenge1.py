import json
import psycopg2

deserialized_json_list = []
non_duplicated_list = []
non_duplicated_tracks = []

def main():
    fileReading()
    connectionDB()
    
def connectionDB():
    # Connect to the database
    try:
        conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=root")
        print ("Successfully connected to the database.")
    except:
        print ("Unable to connect to the database. Please check your options and try again.")
        return

    # Create a cursor for executing queries
    cur = conn.cursor()
    
    #Deleting tables while running code
    cur.execute('DROP TABLE IF EXISTS tracks CASCADE')
    cur.execute('DROP TABLE IF EXISTS playback_events CASCADE')
    
    # Create table tracks
    cur.execute("""create table tracks(
    id varchar(100) primary key not null,
    name varchar(100),
    duration_ms int,
    disc_number int,
    track_number int,
    explicit boolean,
    is_local boolean
    )""")
    
    cur.execute("""create table playback_events(
    track_id varchar(100) not null references tracks(id),
    played_at varchar(100),
    context_type varchar(100),
    context_uri varchar(100)
    )""")
    
    # Display current tables within database
    cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    #Printing current tables
    #print (cur.fetchall())
    
    counter = 0
    #Fetching key values from tracks
    for track in non_duplicated_tracks:
        track_id = non_duplicated_tracks[counter]["id"]
        track_name = non_duplicated_tracks[counter]["name"]
        track_duration = non_duplicated_tracks[counter]["duration_ms"]
        track_disc = non_duplicated_tracks[counter]["disc_number"]
        track_number = non_duplicated_tracks[counter]["track_number"]
        track_explicit = non_duplicated_tracks[counter]["explicit"]
        track_local = non_duplicated_tracks[counter]["is_local"]
        #print(f"Track name: {track_id, track_name,track_duration,track_disc,track_number,track_explicit,track_local}")
        
        #Adding values to tables
        cur.execute("INSERT INTO tracks VALUES (%s, %s, %s, %s, %s, %s, %s)", (track_id, track_name, track_duration, 
        track_disc, track_number, track_explicit, track_local))
        counter += 1
    
    #Insert data into table tracks
    cur.execute('SELECT * FROM tracks')
    rowcount = cur.rowcount
    print (f"Tracks inserted into tracks: {rowcount}")
    #print(cur.fetchall())
    
    counter = 0
    count_except = 0
    track_id = non_duplicated_tracks[counter]["id"]
    #Fetching key values for playback events
    for events in non_duplicated_list:
        try:
            played_at = non_duplicated_list[counter]["played_at"]
            #Finding None values
            played_context_type = None
            played_context_uri = None
            if non_duplicated_list[counter]["context"] is not None:
                played_context_type = non_duplicated_list[counter]["context"]["type"]
                played_context_uri = non_duplicated_list[counter]["context"]["uri"]
        
            cur.execute("INSERT INTO playback_events VALUES (%s, %s, %s, %s)", (track_id, played_at,played_context_type, played_context_uri))
            counter += 1
        except:
            #print(f"Este id trono: {track_id}")
            #count_except += 1
            pass
    #print(count_except)
    #Insert data into table playback_events
    cur.execute('SELECT * FROM playback_events')
    rowcount = cur.rowcount
    print (f"Events inserted into playback events: {rowcount}")
    #print(cur.fetchall())

    conn.commit()
   
   
def fileReading():
    with open("data.json","r") as read_file:
        #Reading line by line the JSON file
        for line in read_file:
            deserialized_json = json.loads(line)
            #Saving deserialized json into a list of dictionaries
            deserialized_json_list.append(deserialized_json)
            
    #Current total count of songs     
    for item in deserialized_json_list:
        if item not in non_duplicated_list:
            non_duplicated_list.append(item) 
    print(f"Non duplicated items: {len(non_duplicated_list)}")
    
    #Cleaning duplicated tracks from cleaned list
    for track in non_duplicated_list:
        new_track = {"id": track["track"]["id"],
                    "name": track["track"]["name"],
                    "duration_ms": track["track"]["duration_ms"],
                    "disc_number": track["track"]["disc_number"],
                    "track_number": track["track"]["track_number"],
                    "explicit": track["track"]["explicit"],
                    "is_local": track["track"]["is_local"]}
        if new_track not in non_duplicated_tracks:
            non_duplicated_tracks.append(new_track)
        #print(non_duplicated_tracks)
    print(f"Non duplicated tracks: {len(non_duplicated_tracks)}")

    return non_duplicated_list

main()
