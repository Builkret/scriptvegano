import mysql.connector
import anthropic
from datetime import datetime

# Configuración de la conexión a la base de datos
db_config = {
    'host': 'locahost pa',
    'user': 'juanito el mistico',
    'password': 'juanito2024',
    'database': 'analisis'
}

# Configurar el cliente de Anthropic
client = anthropic.Client(api_key="esto es un secreto perro")  # Asegúrate de que la clase sea 'Client'

def procesar_compra(compra):
    # Formato del prompt para enviar a Anthropic, siguiendo la convención de "Human" y "Assistant"
    prompt = f"""
    Analiza las siguientes compras de la universidad:

    Denominación: {compra['denominacion']}
    Objeto: {compra['objeto']}
    Monto: {compra['monto']}
    Proveedor: {compra['proveedor']}
    Socio: {compra['socio']}
    Fecha de Inicio: {compra['fecha_inicio']}
    Fecha de Término: {compra['fecha_termino']}

    Por favor, proporciona lo siguiente:
    1. ¿Existe alguna anomalía en el monto?
    2. Categoría de la compra (material educativo, tecnología, servicios, otros).
    3. Calidad de la compra (alta, media o baja).
    4. Relevancia de la compra (relevante, moderadamente relevante, no relevante).
    
    Responde en el siguiente formato:
    Anomalía: [Positivo o Negativo, si es positivo menciona el motivo]
    Categoría: material educativo, tecnología, servicios, cualquier otra categoria, especifica el servicio.
    Calidad: [Descripción de no más de 100 caracteres].
    Relevancia: relevante, moderadamente relevante, no relevante (motivo de no más de 100 caracteres).
    """

    # Llamada a la API de Anthropic
    message = client.messages.create(
        model="claude-3-haiku-20240307",
        max_tokens=1000,
        temperature=0,
        messages=[{"role": "user", "content": prompt}]
    )

    # Extraer el contenido de la respuesta
    message_content = message.content[0].text
    

    # Procesar la respuesta de la IA
    lines = message_content.split('\n')
    anomalia = lines[0].split(': ')[1].strip()
    categoria = lines[1].split(': ')[1].strip()
    calidad = lines[2].split(': ')[1].strip()
    relevancia = lines[3].split(': ')[1].strip()
           
    # Verificar que todos los valores han sido extraídos
    
    return {
        'anomalia': anomalia,
        'categoria': categoria,
        'calidad': calidad,
        'relevancia': relevancia
    }

def main():
    try:
        # Conectar a la base de datos
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        # Obtener compras desde la tabla edu_entrada
        cursor.execute("SELECT * FROM edu_entrada WHERE procesado = 0")
        compras = cursor.fetchall()
        
        for compra in compras:
            try:
                # Procesar la compra con la IA de Anthropic
                resultado = procesar_compra(compra)

                # Insertar el resultado en la tabla edu_resultados
                insert_query = """
                INSERT INTO edu_resultados (id_resultado, anomalia, categoria, calidad, relevancia, fecha_procesado)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
            
                cursor.execute(insert_query, (
                    compra['id'],
                    resultado['anomalia'],
                    resultado['categoria'],
                    resultado['calidad'],
                    resultado['relevancia'],
                    datetime.now()
                ))

                # Marcar la compra como procesada en edu_entrada
                update_query = "UPDATE edu_entrada SET procesado = 1 WHERE id = %s"
                cursor.execute(update_query, (compra['id'],))
                conn.commit()
                print(f"Compra {compra['id']} procesada y actualizada.")
            except Exception as e:
                print(f"Error general al procesar compra {compra['id']}: {e}")

    except mysql.connector.Error as err:
        print(f"Error de base de datos: {err}")
    except Exception as e:
        print(f"Error general: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()
