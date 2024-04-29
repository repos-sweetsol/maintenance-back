from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import psycopg as pg

from datetime import datetime


CONN_ARGS = {
    "host": "localhost",
    "port": "5432",
    "dbname": "sweetsol",
    "user": "postgres",
    "password": "postgres"
}


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins = [ "*" ],
    allow_methods = [ "*" ],
    allow_headers = [ "*" ]
)


@app.get( "/technicians" )
def technicians():
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select * from main.technician
            """)

            res = cur.fetchall()
            res = list( map( lambda e: { "id": e[ 0 ], "name": e[ 1 ] }, res ) )

            return res


@app.get( "/activities" )
def activities():
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select * from main.activity
            """)

            res = cur.fetchall()
            res = list( map( lambda e: { "name": e[ 0 ] }, res ) )

            return res


@app.get( "/areas" )
def areas():
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                select * from main.area
            """)

            res = cur.fetchall()
            res = list( map( lambda e: { "name": e[ 0 ] }, res ) )

            return res


@app.get( "/areas/{area_name}/machines" )
def area_machines( area_name: str ):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select am.mach
                from main.area a, main.area_machine am
                where a.name = am.area
                and am.area = '{area_name}'
            """)

            res = cur.fetchall()
            res = list( map( lambda e: { "name": e[ 0 ] }, res ) )

            return res


@app.get( "/machines/{mach_name}/systems" )
def machine_systems( mach_name: str ):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select ms.sys
                from main.machine m, main.machine_system ms
                where m.name = ms.mach
                and ms.mach = '{mach_name}'
            """)

            res = cur.fetchall()
            res = list( map( lambda e: { "name": e[ 0 ] }, res ) )

            return res


@app.get( "/systems/{sys_name}/subsystems" )
def system_subsystems( sys_name: str ):
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select ss.subsys
                from main.system s, main.system_subsystem ss
                where s.name = ss.sys
                and ss.sys = '{sys_name}'
            """)

            res = cur.fetchall()
            res = list( map( lambda e: { "name": e[ 0 ] }, res ) )

            return res


@app.get( "/failures" )
def failures():
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select * from main.failure
            """)

            res = cur.fetchall()
            res = list( map( lambda e: { "name": e[ 0 ] }, res ) )

            return res


@app.get( "/spares" )
def failures():
    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                select * from main.spare
            """)

            res = cur.fetchall()
            res = list( map( lambda e: { "name": e[ 0 ] }, res ) )

            return res


@app.post( "/forms" )
def forms( form: dict ):
    values = ""
    for k in [ "technician", "activity", "area", "machine", "system", "subsystem", "failure", "description" ]:
        if( form[ k ] == "-" ):
            values += "null,"
        else:
            values += f"'{ form[ k ] }',"
    values = values[ 0 : -1 ]

    date = datetime.strptime( form[ "date" ], "%Y-%m-%d" )

    stime = datetime.strptime( form[ "stime" ], "%H:%M" )
    ftime = datetime.strptime( form[ "ftime" ], "%H:%M" )
    
    stime = datetime.combine( date.date(), stime.time() ).strftime( "%Y-%m-%d %H:%M:%S%z" )
    ftime = datetime.combine( date.date(), ftime.time() ).strftime( "%Y-%m-%d %H:%M:%S%z" )

    values += f",'{ stime }','{ ftime }'"

    with pg.connect( **CONN_ARGS ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                insert into main.maintenance(tech, act, area, mach, sys, subsys, failure, descr, start_time, finish_time) values({values})
            """)