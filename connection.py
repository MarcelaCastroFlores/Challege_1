import psycopg2

def getData():
    # Connect to the database
    try:
        conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=root")
        print ("Successfully connected to the database.")
    except:
        print ("Unable to connect to the database. Please check your options and try again.")
        return

    # Create a cursor for executing queries
    cur = conn.cursor()
    
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
    track_id varchar(100) primary key not null references tracks(id),
    played_at varchar(100),
    context_type varchar(100),
    context_uri varchar(100)
    )""")
    
    # Display current tables within database
    cur.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    print (cur.fetchall())
    #Insert data into table
    cur.execute("INSERT INTO tracks VALUES (%s, %s, %s, %s, %s, %s, %s)", ('Song1', 'La Bikina', 5, 5, 5, True, False ))
    cur.execute('SELECT * FROM tracks')
    print(cur.fetchall())
   # conn.commit()

getData()