
import json
import logging
import pymssql 
import azure.functions as func
import sys, os.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname( __file__ ), '.venv/Lib/site-packages')))

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    conn = pymssql.connect(server='serverapipython.database.windows.net', user='angular', password='Crud246476', database='apipython') 
    cursor = conn.cursor()
    #Funcion para crear
    def post_query(query,cursor):
        try:
            cursor.execute(query)
            conn.commit()
            logging.info("Query successful")
        except:
            logging.info(f"Error",sys.exc_info()[0])
    #Funcion para leer
    def read_query(query, cursor):
        
        try:
            result = None
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except ConnectionAbortedError:
            logging.info('Error de conexion')
            return func.HttpResponse("Fallo la conexion",status_code=404)
    def readOneQuery(query, cursor):
        
        try:
            result = None
            cursor.execute(query)
            result = cursor.fetchone()
            return result
        except ConnectionAbortedError:
            logging.info('Error de conexion')
            return func.HttpResponse("Fallo la conexion",status_code=404)

    #Listar todos
    if req.method=="GET" and req.route_params.get("param1")=="peliculas"  and req.route_params.get("param2")==None:
        query="SELECT id, titulo, puntuacion FROM peliculas"
        result=read_query(query,cursor)
        respuesta=[]
        i=0
        for val in result:
            movie = {"ID":val[0],"Titulo":val[1],"Puntuacion":val[2]}
            respuesta.append(movie)
        s1 = json.dumps(respuesta)
        return func.HttpResponse(s1,status_code=200)
    #Crear
    elif req.method=="POST" and req.route_params.get("param1")=="peliculas":
        body=req.get_json()
        titulo=str(body.get('Titulo'))
        puntuacion=int(body.get('Puntuacion'))
        query=f"INSERT INTO peliculas (titulo, puntuacion) VALUES ('{titulo}', '{puntuacion}');"
        result=post_query(query,cursor)
        s1 = json.dumps({"message":"Pelicula creada"})
        return func.HttpResponse(s1,status_code=200)
    #Obtener por id
    elif req.method=="GET" and req.route_params.get("param1")=="peliculas" :
        idpeli=req.route_params.get("param2")
        if idpeli==None:
            return func.HttpResponse("Ingresa un ID valido",status_code=404)
        elif idpeli is not None:
            query=f"SELECT id, titulo, puntuacion FROM peliculas WHERE id= '{idpeli}'"
            result=readOneQuery(query,cursor)
            if result==None:
                return func.HttpResponse("ID no encontrado",status_code=404)
            elif result is not []:
                logging.info(result)
                result2 = {"ID":result[0],"Titulo":result[1],"Puntuacion":result[2]}
                result3=json.dumps(result2)
                return func.HttpResponse(result3,status_code=200)
    #Borrar
    
    elif req.method=="DELETE" and req.route_params.get("param1")=="peliculas":
 
        
        idpeli=req.route_params.get("param2")
        query=f"SELECT id FROM peliculas WHERE id= '{idpeli}'"
        result=readOneQuery(query,cursor)
        if idpeli==None or result==None:
            return func.HttpResponse("ID no encontrado",status_code=404)
        elif idpeli is not None:
            query=f"delete from peliculas where id='{idpeli}';"
            result=post_query(query,cursor)
            s1 = json.dumps({"message":"Pelicula borrada"})
            return func.HttpResponse(s1,status_code=200)
            
    #Actualizar  

    elif req.method=="PUT" and req.route_params.get("param1")=="peliculas":
        body=req.get_json()
        idpeli=req.route_params.get("param2")
        query=f"SELECT id FROM peliculas WHERE id= '{idpeli}'"
        result=readOneQuery(query,cursor)
        if idpeli==None or result==None:
            return func.HttpResponse("ID no encontrado",status_code=404)
        elif idpeli is not None:
            titulo=str(body.get('Titulo'))
            puntuacion=int(body.get('Puntuacion'))
            query=f"UPDATE peliculas SET titulo = '{titulo}', puntuacion = '{puntuacion}' WHERE id='{idpeli}'; "
            result=post_query(query,cursor)
            s1 = json.dumps({"message":"Pelicula actualizada"})
            return func.HttpResponse(s1,status_code=200)
             
    
    else:
        return func.HttpResponse(status_code=404)
        