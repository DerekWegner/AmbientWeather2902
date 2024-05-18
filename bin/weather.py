#!/usr/bin/env python3
import http.server
import socketserver
import sqlite3
import statsd
import sys
import socket
import time
# import SimpleHTTPServer

# import time

DB_FILE = sys.argv[1] if len(sys.argv)>1 else 'weather.sqlite'
# PORT = 8088
PORT = int(sys.argv[2]) if len(sys.argv)>2 else 8088
VACUUM_SECONDS = 1*60

last_vacuum = int(time.time());
class RequestHandler(http.server.BaseHTTPRequestHandler):
    def log_request(self, format, *args):
        # Don't barf every request to stderr/stdout
        return
    def do_GET(self):
        # global count
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        query_components = dict(qc.split("=") for qc in self.path[2:].split("&"))

        connection = sqlite3.connect(DB_FILE)         # or ':memory:'
        cursor = connection.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS weather
       (
       stationtype text,
       dateutc text PRIMARY KEY,
       tempinf real,
       humidityin INT,
       baromrelin real,
       baromabsin REAL,
       tempf real,
       humidity INT,
       winddir INT,
       windspeedmph REAL,
       windgustmph REAL,
       maxdailygust REAL,
       hourlyrainin REAL,
       eventrainin REAL,
       dailyrainin REAL,
       weeklyrainin REAL,
       monthlyrainin REAL,
       totalrainin REAL,
       solarradiation REAL,
       uv INT,
       batt_co2 INT
       );
''')
       # for key, value in query_components.items():
       #    c.execute("INSERT INTO data VALUES (?,?)", (key, value))
       # connection.commit()

        cursor.execute('''INSERT OR REPLACE INTO weather (
stationtype,
dateutc, 
tempinf, 
humidityin, 
baromrelin,
baromabsin,
tempf,humidity,winddir,windspeedmph,windgustmph,maxdailygust,hourlyrainin,eventrainin,dailyrainin,weeklyrainin,monthlyrainin,totalrainin,solarradiation,uv,batt_co2
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
''',
                  (
                      query_components.get("ata/report/stationtype"),
                      query_components.get("dateutc").replace('+', ' '),
                      query_components.get("tempinf"),
                      query_components.get('humidityin'),
                      query_components.get('baromrelin'),
                      query_components.get('baromabsin'),
                      query_components.get('tempf'),
                      query_components.get('humidity'),
                      query_components.get('winddir'),
                      query_components.get('windspeedmph'),
                      query_components.get('windgustmph'),
                      query_components.get('maxdailygust'),
                      query_components.get('hourlyrainin'),
                      query_components.get('eventrainin'),
                      query_components.get('dailyrainin'),
                      query_components.get('weeklyrainin'),
                      query_components.get('monthlyrainin'),
                      query_components.get('totalrainin'),
                      query_components.get('solarradiation'),
                      query_components.get('uv'),
                      query_components.get('batt_co2')
                  )
                  )
                  
        connection.commit()

        global last_vacuum
        now = int(time.time())
        print(f"now {now}, last_vacuum:{last_vacuum}")
        
        
        if(now - last_vacuum >= VACUUM_SECONDS):
            print("Doing a vacuum");
            connection.execute('vacuum;')
            last_vacuum = now;


        # See https://stackoverflow.com/questions/858623/how-to-recognize-whether-a-script-is-running-on-a-tty
        if sys.stdout.isatty():
            number_records = cursor.execute("Select Count(*) from weather");
            print(f"{query_components.get('dateutc')} number_records:{number_records.fetchone()[0]} tempf:{query_components.get('tempf')} tempinf:{query_components.get('tempinf')} windspeedmph:{query_components.get('windspeedmph')} windgustmph:{query_components.get('windgustmph')} hourlyrainin:{query_components.get('hourlyrainin')}")
            # print(query_components);
            number_records.close()
        cursor.close()
        connection.close()

class MyTCPServer(socketserver.TCPServer):
    def server_bind(self):
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)

# with socketserver.TCPServer(("", PORT), RequestHandler) as httpd:
# See https://stackoverflow.com/questions/6380057/python-binding-socket-address-already-in-use

# Entry point. See https://docs.python.org/3/library/__main__.html

def main() -> int:
    """Whatever"""
    print("In main")
    with MyTCPServer(("", PORT), RequestHandler) as httpd:
        try:
            print(F"Storing into {DB_FILE}, listening on port {PORT}, VACUUM_SECONDS: {VACUUM_SECONDS}")
            httpd.serve_forever()
        finally:
            print("In finally, shutdown httpd");
            httpd.shutdown()
            httpd.server_close()
            # time.sleep(2)
            print("In finally, done");
    return 0

if __name__ == '__main__':
    sys.exit(main())
