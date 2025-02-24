import pyodbc
from openai import AzureOpenAI 
 

# Database connection details
SERVER = 'IP'
DATABASE = 'DB'
USERNAME = 'USER'
PASSWORD = 'PASSWORD'
DRIVER = 'ODBC Driver 18 for SQL Server'

# Build connection string
connection_string = (
    f'DRIVER={{{DRIVER}}};'
    f'SERVER={SERVER};'
    f'DATABASE={DATABASE};'
    f'UID={USERNAME};'
    f'PWD={PASSWORD};'
    'TrustServerCertificate=yes'
)
def create_connection():
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        print("✅ Conectado al SQL Server!\n")
        return conn, cursor
    except pyodbc.Error as e:
        print(f"❌ Error conectando al SQL Server: {str(e)}\n")
        return None, None
try:
    # Establish connection
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print("✅ Conectado al SQL Server!\n")
except pyodbc.Error as e:
    print(f"❌ Error conectando al SQL Server: {str(e)}\n")

# OpenAI API Client
client = AzureOpenAI(
    api_key="API KEY",
    api_version=" VERSION DE LA API ",
    azure_endpoint="ENDPOINT DE AZURE/OPENAI"
)
def clean_sql_query(query):
    """Limpia el query de SQL para evitar errores por como da el output el GPT, tanto como prefijos como caracteres extra"""
    # Remove common prefixes that GPT might add
    prefixes_to_remove = ['sql', 'SQL:', 'Query:', 'sql:', 'SQL Query:']
    cleaned_query = query.strip()
    
    for prefix in prefixes_to_remove:
        if cleaned_query.lower().startswith(prefix.lower()):
            cleaned_query = cleaned_query[len(prefix):].strip()
    
    # Remove backticks and other unwanted characters
    cleaned_query = cleaned_query.replace('', '').strip()
    
    return cleaned_query
# Generate SQL Query from Natural Language
def generate_sql_query(user_question):
    messages = [
        {
            "role": "system",
            "content": (
                "Eres un experto en SQL, y tu trabajo es traducir las consultas en lenguaje natural de los usuarios a queries de SQL, "
                "mostrando tanto el resultado como la consulta en sí. Estas utilizando la siguiente base de datos de SQL Server, dentro del esquema dbo. Cabe destacar que, en los resultados numericos, en el resultado que se muestra, "
                "no es necesario especificar si es decimal o entero, y si es necesario aplicar la cantidad apropiada de separadores numericos\n\n"
                "📌 **Base de datos:** AdventureWorksDW2019\n\n"
                "📊 **Tablas y Campos:**\n"
                "**Compras**:\n"
                "- Id. de la fila\n"
                "- Id. del pedido\n"
                "- Fecha del pedido\n"
                "- Fecha de envio\n"
                "- Método de envio\n"
                "- Id. del cliente\n"
                "- Nombre del cliente\n"
                "- Segmento\n"
                "- Ciudad\n"
                "- Provincia/Estado/Departamento\n"
                "- Pais/Region\n"
                "- Region\n"
                "- Id. del producto\n"
                "- Categoria\n"
                "- Subcategoria\n"
                "- Nombre_del_producto\n"
                "- Ventas\n"
                "- Cantidad\n"
                "- Descuento\n"
                "- Ganancia\n\n"
                "**Devoluciones**:\n"
                "- Id. del pedido\n"
                "- Devuelto\n\n"
                "**Personas**:\n"
                "- Region\n"
                "- Gerente regional\n\n"
                "📌 **Relaciones:**\n"
                "- Compras se relaciona con Devoluciones mediante Id. del pedido.\n"
                "- Compras se relaciona con Personas mediante Region."
                "El resultado debe ser retornado en tipo varchar/texto "
                
            )
        },
        {
            "role": "user",
            "content": (
                f"Convierte la siguiente consulta a un query de SQL Server, mostrando solo el query (sin backticks ni explicaciones):\n\n"
                f"{user_question}"
            )
        }
    ]

    try:
        response = client.chat.completions.create(
            model="gpt-4o", 
            messages=messages,
            max_tokens=200,
            temperature=0.2
        )
        
        sql_query = response.choices[0].message.content.strip()
        
        # Remove backticks from the query (if present)
        sql_query = sql_query.replace("", "")  # Replace backticks with empty string
        
        return clean_sql_query(sql_query)  # Add this line to clean the query
    except Exception as e:
        print(f"❌ OpenAI API Error: {str(e)}")
        return None
    
def format_result_row(row):
    """Format a single result row for display"""
    return " | ".join(str(value).strip() if value is not None else 'NULL' for value in row)

def execute_and_display_query(cursor, sql_query):
    """Execute SQL query and display results in a formatted way"""
    if not sql_query:
        print("   No se generó ningún query SQL debido a un error.")
        return

    try:
        cursor.execute(sql_query)
        results = cursor.fetchall()
        
        print("\n📊 Resultados de la consulta:")
        if results:
            # Get column names
            columns = [column[0] for column in cursor.description]
            
            # Print headers
            print("\n" + " | ".join(columns))
            print("-" * (sum(len(col) for col in columns) + 3 * (len(columns) - 1)))
            
            # Print rows
            for row in results:
                print(format_result_row(row))
            
            print(f"\nTotal de resultados: {len(results)}")
        else:
            print("   No se encontraron resultados.")
            
    except Exception as e:
        print(f"❌ Error al ejecutar la consulta: {str(e)}")

def chat_with_sqlbot():
    conn, cursor = create_connection()
    if not conn or not cursor:
        return

    print("\n💬 **Maquinola esta listo!** (Escribe 'exit/salir/quit' para salir)\n")
    
    try:
        while True:
            user_question = input("👤 Tu: ").strip()
            
            if user_question.lower() in ["exit", "salir", "quit"]:
                print("\n👋 Cerrando chat. Hasta luego!")
                break

            sql_query = generate_sql_query(user_question)
            
            if sql_query:
                print(f"\n🤖 Maquinola: Aqui esta tu consulta SQL:\n📝 {sql_query}")
                execute_and_display_query(cursor, sql_query)
                print()  # Add empty line for readability
            else:
                print("❌ No es posible generar consulta SQL, intente de nuevo.\n")
    
    finally:
        cursor.close()
        conn.close()
        print("✅ Conexión al SQL Server terminada.")

if __name__ == "__main__":
    chat_with_sqlbot()
